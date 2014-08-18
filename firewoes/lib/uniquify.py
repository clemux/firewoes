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

from firewoes.lib import hash
from firehose.model import (_string_type, Message, Notes, Point,
                            CustomFields, File, Function, Generator,
                            Hash, Location, Sut, DebianSource, DebianBinary,
                            SourceRpm, Stats, Range,
                            Metadata, Trace, Analysis, State,
                            Issue, Failure, Info)

from firewoes.lib.orm import (t_message, t_notes, t_point, t_customfields,
                              t_file, t_function, t_generator, t_hash,
                              t_location, t_sut, t_stats, t_range,
                              t_metadata, t_trace, t_analysis,
                              t_state, t_result)

from sqlalchemy import select, exists, literal

from collections import defaultdict



# performance will probably not be great with such objects,
# but they're easier to manipulate for now

# TODO: use weakref for `parent` and `siblings`
# TODO: profile memory use (guppy)
# TODO: use a lighter data structure


class Node(object):

    def __init__(self, obj, parent, attr_name, siblings=None):
        """
        `obj`: firehose object
        `parent`: Node object
        `attr_name`: name of the corresponding attribute
                     in the parent firehose object
        `siblings`: parent's other children
        """

        self.obj = obj
        self.parent = parent
        self.attr_name = attr_name # my name in the parent node
        self.children = list()
        if siblings is not None:
            self.siblings = siblings
        self.children_filled = False


    def fill_children(self):
        # from original implementation
        for (attr_name, attr) in hash.get_attrs(self.obj):
            if isinstance(attr, list):
                self.children.append(ListNode(attr, self, attr_name, self.children))
            elif (type(attr) not in (int, float, str, _string_type)
                  and attr is not None):
                node = Node(attr, self, attr_name, self.children)
                node.set_fk()
                self.children.append(node)

        self.children_filled = True

    def set_fk(self):
        attr_name = self.attr_name + '_id'
        setattr(self.parent.obj, attr_name, self.obj.id)

    @property
    def key(self):
        return self.obj.__class__, self.obj.id

    def __repr__(self):
        return '<Node: %s/%s>' % (self.obj.__class__, self.obj.id)


class ListNode(Node):
    """
    Some attributes are actually list of firehose objets.
    A ListNode represents such a list, faking a normal node.
    `obj` is a list of attributes.
    """

    def fill_children(self):
        self.children = list()
        for item in self.obj:
            node = Node(item, self, None, self.children)
            self.set_fk(node)
            self.children.append(node)
        self.children_filled = True

    def set_fk(self, node):
        attr_name = self.parent.obj.__class__.__name__.lower() + '_id'
        setattr(node.obj, attr_name, self.parent.obj.id)


    @property
    def key(self):
        """ I'm not a firehose element, so I have no key """
        return None

    def __repr__(self):
        return '<ListNode: %s>' % self.attr_name

def uniquify(engine, obj):
    # root node (`Analysis` object)
    # so no parent, and no attribute name, and no siblings
    current = Node(obj, None, None, None)
    depth = 0 # DEBUG

    data = defaultdict(set)


    while True:
        if not current.children_filled:
            current.fill_children()

        # (1) if children, go back to (1) with first child
        # (2) process self
        # (3) if siblings, go back to (1) with first sibling

        if len(current.children) > 0:
            current = current.children.pop()
            depth += 1 # DEBUG

        elif current.parent is not None:
            if current.key is not None:
                klass, hash = current.key
                data[klass].add(current.obj)
            if len(current.siblings) > 0:
                current = current.siblings.pop()
            else:
                current = current.parent
                depth -= 1 # DEBUG

        else:
            klass, hash = current.key
            data[klass].add(current.obj)
            break

    objects = [Notes, Point, CustomFields, Hash, File, Function,
               Generator, Range, Sut, Stats, Location, Metadata, Trace,
               Location, Message, Analysis, State]
    # TODO: find pertinent name
    # TODO: returns functions that creates the SQL queries instead
    #       this would allow easier factorization of specific rules
    #       for tables holding different firehose objects with a
    #       'type' field
    # attr_type => ([columns], table)
    rules = {
        Notes: (['id', 'text'], t_notes),
        Point: (['id', 'line', 'column'], t_point),
        CustomFields: (['id'], t_customfields),
        Hash: (['id', 'alg', 'hexdigest'], t_hash),
        File: (['id', 'givenpath', 'abspath', 'hash_id'], t_file),
        Function: (['id', 'name'], t_function),
        Generator: (['id', 'name', 'version'], t_generator),
        Sut: (['id', 'type', 'name', 'version', 'release', 'buildarch'], t_sut),
        Stats: (['id', 'wallclocktime'], t_stats),
        Range: (['id', 'start_id', 'end_id'], t_range),
        Metadata: (['id', 'generator_id', 'sut_id', 'file_id', 'stats_id'],
                   t_metadata),
        Trace: (['id'], t_trace),
        Location: (['id', 'file_id', 'function_id', 'point_id', 'range_id'],
                   t_location),
        Message: (['id', 'text'], t_message),
        Analysis: (['id', 'metadata_id', 'customfields_id'], t_analysis),
        State: (['id', 'trace_id', 'location_id', 'notes_id'], t_state),
        }

    result_base_columns = ['analysis_id', 'location_id', 'message_id',
                           'customfields_id']
    result_columns = {
        Issue: result_base_columns + ['notes_id', 'trace_id', 'testid', 'severity'],
        Failure: result_base_columns,
        Info: result_base_columns,
    }

    conn = engine.connect()
    trans = conn.begin()

    # TODO: this does not work, which forces us to fill tables in a specific
    #       order, and thus to take time filling the `data` dictionary,
    #       instead of directly executing the SQL queries
    #       * this might or might be relevant for real-word performance
    conn.execute("set constraints all deferred;")

    for attr_cls in objects:
        attrs, table = rules[attr_cls]
        for i in data[attr_cls]:
            # from http://stackoverflow.com/a/18605162/1200503
            literals = [literal(getattr(i, attr)) for attr in attrs]
            sel = select(literals).where(
                ~exists([table.c.id]).where(table.c.id == i.id)
            )
            ins = table.insert().from_select(attrs, sel)
            conn.execute(ins)

    for issue in data[Issue]:
        literals = [literal(getattr(issue, 'id')), literal('issue')]
        literals.extend([literal(getattr(issue, attr)) for attr in result_columns[Issue]])
        attrs = ['id', 'type'] + result_columns[Issue]
        sel = select(literals).where(
            ~exists([t_result.c.id]).where(t_result.c.id == i.id)
        )
        ins = t_result.insert().from_select(attrs, sel)
        conn.execute(ins)

    for issue in data[Info]:
        literals = [literal(getattr(issue, 'id')), literal('info')]
        literals.extend([literal(getattr(issue, attr)) for attr in result_columns[Info]])
        attrs = ['id', 'type'] + result_columns[Info]
        sel = select(literals).where(
            ~exists([t_result.c.id]).where(t_result.c.id == i.id)
        )
        ins = t_result.insert().from_select(attrs, sel)
        conn.execute(ins)

    for issue in data[Failure]:
        literals = [literal(getattr(issue, 'id')), literal('failure')]
        literals.extend([literal(getattr(issue, attr)) for attr in result_columns[Failure]])
        attrs = ['id', 'type'] + result_columns[Failure]
        sel = select(literals).where(
            ~exists([t_result.c.id]).where(t_result.c.id == i.id)
        )
        ins = t_result.insert().from_select(attrs, sel)
        conn.execute(ins)

    sut_columns = ['name', 'version', 'release', 'buildarch']
    for sut in data[DebianSource]:
        attrs = ['id', 'type'] + sut_columns
        literals = [literal(getattr(sut, 'id')), literal('debian-source')]
        literals.extend([literal(getattr(sut, attr)) for attr in sut_columns])
        sel = select(literals).where(
            ~exists([t_sut.c.id]).where(t_sut.c.id == i.id)
        )
        ins = t_sut.insert().from_select(attrs, sel)
        conn.execute(ins)


    for sut in data[DebianBinary]:
        attrs = ['id', 'type'] + sut_columns
        literals = [literal(getattr(sut, 'id')), literal('debian-binary')]
        literals.extend([literal(getattr(sut, attr)) for attr in sut_columns])
        sel = select(literals).where(
            ~exists([t_sut.c.id]).where(t_sut.c.id == i.id)
        )
        ins = t_sut.insert().from_select(attrs, sel)
        conn.execute(ins)

    for sut in data[SourceRpm]:
        attrs = ['id', 'type'] + sut_columns
        literals = [literal(getattr(sut, 'id')), literal('source-rpm')]
        literals.extend([literal(getattr(sut, attr)) for attr in sut_columns])
        sel = select(literals).where(
            ~exists([t_sut.c.id]).where(t_sut.c.id == i.id)
        )
        ins = t_sut.insert().from_select(attrs, sel)
        conn.execute(ins)

    trans.commit()
    conn.close()

    return current.obj
