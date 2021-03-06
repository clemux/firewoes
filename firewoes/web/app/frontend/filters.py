# Copyright (C) 2013  Matthieu Caneill <matthieu.caneill@gmail.com>
# Copyright (C) 2014  Clement Schreiner <clement@mux.me>
#
# This file is part of Firewoes.
#
# Firewoes is free software: you can redistribute it and/or modify it under
# the terms of the GNU Affero General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU Affero General Public License for more
# details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


from models import to_dict

from firewoes.lib.orm import Analysis, Issue, Failure, Info, Result, \
    Generator, Sut, Metadata, Message, Location, File, Point, Range, Function
from firewoes.lib.debianutils import DebianPackagePeopleMapping, \
    emails_for_person

from sqlalchemy import func, desc, and_

class Menu(object):
    """
    A menu is a set of active and inactive filters.
    It permits to obtain a drill-down menu, as long as an SQLAlchemy
    filter to query the results.
    """
    def __init__(self, active_filters_dict):
        """
        Creates a menu.
        The active_filters_dict contains the already activated filters,
        in the form name=value, e.g. generator_name="coccinelle".
        """
        self.filters = []
        self.clauses = []
        
        # we clean the active filters, removing blank values:
        self.active_filters_dict = dict((k, v)
                                        for (k, v) in active_filters_dict.items()
                                        if v != "")
        
        # we create the filters:
        for filter_ in all_filters:
            if filter_[0] in self.active_filters_dict.keys():
                # it's an active filter
                new_filter = filter_[1](
                    value=self.active_filters_dict[filter_[0]],
                    active=True,
                    name=filter_[0])
            
            else: # it's an inactive one
                new_filter = filter_[1](active=False, name=filter_[0])
            # avoids adding non-relevant filters regarding the context:
            if new_filter.is_relevant(
                active_keys=self.active_filters_dict.keys()):
                
                self.filters.append(new_filter)
                if new_filter.is_active():
                    self.clauses += new_filter.get_clauses()
            else:
                # if there was an irrelevant filter in active_filters_dict,
                # we remove it:
                try:
                    del(self.active_filters_dict[new_filter.name])
                except:
                    pass

    
    def filter_sqla_query(self, query):
        """
        Filters the query by all active filters, and returns a new
        SQLAlchely query.
        """
        query = query.filter(and_(*self.clauses))
        return query
    
    def get(self, session, max_items=None):
        """
        Returns the menu in form of a list of filters.
        Needs a SQLAlchemy session for the filters, in their items generation.
        """
        return [filter_.get(session, self.active_filters_dict,
                            clauses=self.clauses, max_items=max_items)
                for filter_ in self.filters]
    
    def __repr__(self):
        string = "MENU:\n"
        for filter_ in self.filters:
            string += "  " + filter_.__repr__() + "\n"
        return string

class Filter(object):
    _cool_name = None
    
    def __init__(self, value=None, active=False, name=None):
        """
        Creates a new filter.
        value is its value in case active=True
        """
        self.value = value
        self.active = active
        if not active:
            self.items = []
            self.is_sliced = False # sliced if max_items > number of items
        self.name = name
    
    def get_clauses(self):
        """
        Returns the SQLAlchemy clauses for this filter.
        """
        raise NotImplementedError
    
    def get_items(self, session, clauses=None, max_items=None):
        """
        Returns the subitems of the menu (only for inactive filters).
        """
        raise NotImplementedError
    
    def get(self, session, active_filters_dict, clauses=None, max_items=None):
        """
        Returns the filter with its attributes.
        """
        res = dict(active=self.active,
                   name=self._cool_name or self.name)
        
        if not self.active:
            res["items"] = self.get_items(session, clauses=clauses,
                                          max_items=max_items)
            res["is_sliced"] = self.is_sliced
            
            # for each item we add its link:
            for item in res["items"]:
                item["link"] = dict(active_filters_dict.items()
                                + [(self.name, item["value"])])
        else:
            res["value"] = self.value
            # we add the "remove" link:
            res["link"] = dict((k, v) for k, v in active_filters_dict.items()
                               if k != self.name)
        return res
    
    def is_relevant(self, active_keys=None):
        """
        Returns True if the filter must be used in this context.
        For filters which implement this, should return False if a
        dependency is not satisfied (e.g. generator_version depends on
        generator_name)
        """
        raise NotImplementedError
    
    def is_active(self):
        return self.active
    
    def __repr__(self):
        string = self.__class__.__name__ + ":\n"
        string += "    active: " + str(self.active) + "\n"
        if not self.active:
            string += "    items: "
            for item in self.items:
                string += item
            
        return string

class FilterFirehoseAttribute(Filter):
    def is_relevant(self, active_keys=None):
        for dep in self._dependencies:
            if dep not in active_keys:
                return False
        return True
    
    def joins(self, query):
        raise NotImplementedError
    
    def group_by(self, session, attribute, outerjoins, clauses=None,
                 max_items=None):
        query = session.query(attribute.label("value"),
                              func.count(Result.id).label("count"))
        
        for join in outerjoins:
            query = query.outerjoin(join[0], join[1])
        
        if clauses is not None:
            query = query.filter(and_(*clauses))
        query = (query
               .group_by(attribute)
               .order_by(desc("count"))
                 )

        # slicing
        if max_items is not None:
            number_of_all_results = query.count()
            if number_of_all_results > max_items:
                query = query.limit(max_items)
                self.is_sliced = True
        
        return query.all()

##################################################
# real world filters:
##################################################

### ERROR TYPE ###

class FilterErrorType(FilterFirehoseAttribute):
    _dependencies = []
    _outerjoins = [
        (Location, Result.location_id==Location.id),
        (File, Location.file_id==File.id),
        (Function, Location.function_id==Function.id),
        (Analysis, Result.analysis_id == Analysis.id),
        (Metadata, Analysis.metadata_id==Metadata.id),
        (Sut, Metadata.sut_id==Sut.id),
        (Generator, Metadata.generator_id==Generator.id),
        ]
    _cool_name = "Error type"
    
    def get_clauses(self):
        return [(Result.type == self.value)]
    
    def get_items(self, session, clauses=None, max_items=None):
        res = self.group_by(session, Result.type, self._outerjoins,
                            clauses=clauses, max_items=max_items)
        return to_dict(res)

### GENERATOR ###

class FilterGenerator(FilterFirehoseAttribute):
    _outerjoins = [
        (Metadata, Metadata.generator_id==Generator.id),
        (Sut, Metadata.sut_id==Sut.id),
        (Analysis, Analysis.metadata_id == Metadata.id),
        (Result, Result.analysis_id == Analysis.id),
        (Location, Result.location_id==Location.id),
        (Function, Location.function_id==Function.id),
        (File, Location.file_id==File.id),
        ]

class FilterGeneratorName(FilterGenerator):
    _dependencies = []
    _cool_name = "Generator"
    
    def get_clauses(self):
        return [(Generator.name == self.value)]
    
    def get_items(self, session, clauses=None, max_items=None):
        res = self.group_by(session, Generator.name, self._outerjoins,
                            clauses=clauses, max_items=max_items)
        return to_dict(res)
    
class FilterGeneratorVersion(FilterGenerator):
    _dependencies = ["generator_name"]
    _cool_name = "Generator version"
    
    def get_clauses(self):
        return [(Generator.version == self.value)]
    
    def get_items(self, session, clauses=None, max_items=None):
        res = self.group_by(session, Generator.version, self._outerjoins,
                            clauses=clauses, max_items=max_items)
        return to_dict(res)

### SUT ###

class FilterSut(FilterFirehoseAttribute):
    _outerjoins = [
        (Metadata, Metadata.sut_id==Sut.id),
        (Generator, Metadata.generator_id==Generator.id),
        (Analysis, Analysis.metadata_id == Metadata.id),
        (Result, Result.analysis_id == Analysis.id),
        (Location, Result.location_id==Location.id),
        (Function, Location.function_id==Function.id),
        (File, Location.file_id==File.id),
        ]

class FilterSutType(FilterSut):
    _dependencies = []
    _cool_name = "Type"
    
    def get_clauses(self):
        return [(Sut.type == self.value)]
    
    def get_items(self, session, clauses=None, max_items=None):
        res = self.group_by(session, Sut.type, self._outerjoins,
                            clauses=clauses, max_items=max_items)
        return to_dict(res)


class FilterSutName(FilterSut):
    _dependencies = []
    _cool_name = "Package"
    
    def get_clauses(self):
        return [(Sut.name == self.value)]
    
    def get_items(self, session, clauses=None, max_items=None):
        res = self.group_by(session, Sut.name, self._outerjoins,
                            clauses=clauses, max_items=max_items)
        return to_dict(res)

class FilterSutVersion(FilterSut):
    _dependencies = ["sut_name"]
    _cool_name = "Package version"
    
    def get_clauses(self):
        return [(Sut.version == self.value)]
    
    def get_items(self, session, clauses=None, max_items=None):
        res = self.group_by(session, Sut.version, self._outerjoins,
                            clauses=clauses, max_items=max_items)
        return to_dict(res)

class FilterSutRelease(FilterSut):
    _dependencies = ["sut_name"]
    _cool_name = "Package release"
    
    def get_clauses(self):
        return [(Sut.release == self.value)]
    
    def get_items(self, session, clauses=None, max_items=None):
        res = self.group_by(session, Sut.release, self._outerjoins,
                            clauses=clauses, max_items=max_items)
        return to_dict(res)

class FilterSutBuildarch(FilterSut):
    _dependencies = ["sut_name"]
    _cool_name = "Package buildarch"
    
    def get_clauses(self):
        return [(Sut.buildarch == self.value)]
    
    def get_items(self, session, clauses=None, max_items=None):
        res = self.group_by(session, Sut.buildarch, self._outerjoins,
                            clauses=clauses, max_items=max_items)
        return to_dict(res)

### LOCATION ###

class FilterLocationFile(FilterFirehoseAttribute):
    _dependencies = ["sut_name"]
    _outerjoins = [
        (Location, Location.file_id==File.id),
        (Function, Location.function_id==Function.id),
        (Result, Result.location_id==Location.id),
        (Analysis, Result.analysis_id == Analysis.id),
        (Metadata, Analysis.metadata_id==Metadata.id),
        (Sut, Metadata.sut_id==Sut.id),
        (Generator, Metadata.generator_id==Generator.id),
        ]
    _cool_name = "File"
    
    def get_clauses(self):
        return [(File.givenpath == self.value),
                (Location.file_id==File.id)]
    
    def get_items(self, session, clauses=None, max_items=None):
        res = self.group_by(session, File.givenpath, self._outerjoins,
                            clauses=clauses, max_items=max_items)
        return to_dict(res)

class FilterLocationFunction(FilterFirehoseAttribute):
    _dependencies = ["sut_name", "location_file"]
    _outerjoins = [
        (Location, Location.function_id==Function.id),
        (File, Location.file_id==File.id),
        (Result, Result.location_id==Location.id),
        (Analysis, Result.analysis_id == Analysis.id),
        (Metadata, Analysis.metadata_id==Metadata.id),
        (Sut, Metadata.sut_id==Sut.id),
        (Generator, Metadata.generator_id==Generator.id),
        ]
    _cool_name = "Function"
    
    def get_clauses(self):
        return [(Function.name == self.value),
                (Location.function_id == Function.id)]
    
    def get_items(self, session, clauses=None, max_items=None):
        res = self.group_by(session, Function.name, self._outerjoins,
                            clauses=clauses, max_items=max_items)
        return to_dict(res)

### TESTID ###

class FilterTestId(FilterFirehoseAttribute):
    _dependencies = ["generator_name"]
    _outerjoins = [
        (Location, Result.location_id==Location.id),
        (File, Location.file_id==File.id),
        (Function, Location.function_id==Function.id),
        (Analysis, Result.analysis_id == Analysis.id),
        (Metadata, Analysis.metadata_id==Metadata.id),
        (Sut, Metadata.sut_id==Sut.id),
        (Generator, Metadata.generator_id==Generator.id),
        ]
    _cool_name = "Test id"
    
    def get_clauses(self):
        return [(Result.testid == self.value)]
    
    def get_items(self, session, clauses=None, max_items=None):
        res = self.group_by(session, Result.testid, self._outerjoins,
                            clauses=clauses, max_items=max_items)
        return to_dict(res)

### BY DEVELOPER ###

# currently only for Debian
class FilterByMaintainerPackages(Filter):
    _cool_name = "Maintainer"
    
    def get_clauses(self):
        return [(DebianPackagePeopleMapping.maintainer_email.in_(
                 emails_for_person(self.value))),
                (Sut.name == DebianPackagePeopleMapping.package_name)]
    
    def get_items(self, session, clauses=None, max_items=None):
        return []
    
    def is_relevant(self, active_keys=None):
        return True


class FilterAnalysisId(FilterFirehoseAttribute):
    _dependencies = []
    _outerjoins = [
        (Location, Result.location_id==Location.id),
        (File, Location.file_id==File.id),
        (Function, Location.function_id==Function.id),
        (Analysis, Result.analysis_id == Analysis.id),
        (Metadata, Analysis.metadata_id==Metadata.id),
        (Sut, Metadata.sut_id==Sut.id),
        (Generator, Metadata.generator_id==Generator.id),
        ]
    _cool_name = "Analysis id"

    def get_clauses(self):
        return [(Result.analysis_id == self.value)]

    def get_items(self, session, clauses=None, max_items=None):
        res = self.group_by(session, Result.analysis_id, self._outerjoins,
                            clauses=clauses, max_items=max_items)
        return to_dict(res)


all_filters = [
    ("maintainer", FilterByMaintainerPackages),
    ("type", FilterErrorType),
    ("generator_name", FilterGeneratorName),
    ("generator_version", FilterGeneratorVersion),
    ("sut_type", FilterSutType),
    ("sut_name", FilterSutName),
    ("sut_version", FilterSutVersion),
    ("sut_release", FilterSutRelease),
    ("sut_buildarch", FilterSutBuildarch),
    ("location_file", FilterLocationFile),
    ("location_function", FilterLocationFunction),
    ("testid", FilterTestId),
    ("analysis_id", FilterAnalysisId),
    ]



if __name__ == "__main__":
    menu = Menu(dict(generator_name="coccinelle"))
    print(menu)
