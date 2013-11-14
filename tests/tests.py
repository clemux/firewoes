import os
import sys
import unittest
import json
from glob import glob

testsdir = os.path.dirname(os.path.abspath(__file__))

from firewoes.lib import orm
from firewoes.bin import firewoes_fill_db
from firewoes.web.app import engine, session, app

class FirewoesTestCase(unittest.TestCase):
    ClassIsSetup = False
    
    def setUp(self):
        # from http://stezz.blogspot.fr
        # /2011/04/calling-only-once-setup-in-unittest-in.html
        
        # If it was not setup yet, do it
        if not self.ClassIsSetup:
            print "Initializing testing environment"
            # run the real setup
            self.setupClass()
            # remember that it was setup already
            self.__class__.ClassIsSetup = True
    
    def setupClass(self):
        app.config["TESTING"] = True
        
        # we fill firewoes_test with our testing data:
        print("Filling db: %s" % app.config["DATABASE_URI"])
        xml_files = glob(testsdir + "/data/*.xml")
        firewoes_fill_db.read_and_create(app.config['DATABASE_URI'],
                                         xml_files, drop=True, echo=False,
                                         engine=engine, session=session)
        
        # TODO: test pack_people_mapping with a short file
        
        self.__class__.app = app.test_client()
        self.__class__.config = app.config
        
    def tearDown(self):
        pass
        
    def test_static_pages(self):
        rv = self.app.get('/')
        assert 'Firewoes' in rv.data

    def test_packages_suggestions(self):
        rv = json.loads(self.app.get('/api/search/?sut_name=pyth').data)
        assert rv['suggestions'][0]["sut_name"] == "python-ethtool"
        
    def test_maintainers_suggestions(self):
        # TODO
        assert True
        
    def test_home_links(self):
        rv = self.app.get('/')
        lstr = '<li><a href="/search/?generator_name=cppcheck">cppcheck</a></li>'
        assert lstr in rv.data
        assert 'python-ethtool</a> (18)' in rv.data
        # to test here: random results (...)
        
    def test_search_list_variables(self):
        rv = json.loads(self.app.get('/api/search/').data)
        assert rv['results_range'] == [1, 10]
        assert rv['offset'] == self.config["SEARCH_RESULTS_OFFSET"]
        assert rv['page'] == 1
        assert rv['suggestions'] == []
        assert rv['results_all_count'] == 18
        
    def test_search_list_root(self):
        rv = json.loads(self.app.get('/api/search/').data)
        assert rv["results_all_count"] == 18
        assert len(rv["results"]) == app.config["SEARCH_RESULTS_OFFSET"]
        
    def test_search_list_testid(self):
        rv = json.loads(self.app.get('/api/search/?generator_name=cpychecker'
                                     '&testid=null-ptr-argument').data)
        assert rv["results"][0]["location_function"] == "get_interfaces_info"
        assert rv["results"][0]["testid"] == "null-ptr-argument"
        
    def test_search_list_location(self):
        rv = json.loads(self.app.get('/api/search/?generator_name=cpychecker'
                                     '&sut_name=python-ethtool'
                                     '&location_file=python-ethtool%2Fethtool.c'
                                     '&sut_version=0.8'
                                     '&location_function=get_ufo').data)
        assert rv["results"][0]["location_function"] == "get_ufo"
        
    def test_drilldownmenu_root(self):
        rv = json.loads(self.app.get('/api/search/').data)
        assert rv["menu"][1] == {
                  "active": False, 
                  "items": [
                {
                    "count": 18, 
                    "link": {
                        "type": "issue"
                        }, 
                    "value": "issue"
                    }
                ], 
                  "is_sliced": False, 
                  "name": "Error type"
                  }
        
    def test_drilldownmenu_active(self):
        rv = json.loads(self.app.get('/api/search/?'
                                     'generator_name=cpychecker'
                                     '&sut_name=python-ethtool'
                                     '&sut_version=0.8'
                                     '&location_file=python-ethtool%2Fethtool.c'
                                     ).data)
        assert rv["menu"][2]["active"] == True
        
    def test_reports(self):
        rv = json.loads(self.app.get('/api/report/python-ethtool/').data)
        
        assert rv["results"][1]["report"]["count_per_generator"] ==  [
            dict(count=18, name="cpychecker"),
            dict(count=0, name="gcc")
            ]
        assert rv["results"][0]["package"]["name"] == "python-ethtool"

if __name__ == '__main__':
    unittest.main()
