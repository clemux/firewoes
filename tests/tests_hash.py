from firewoes.web.app import app
from firewoes.lib.dbutils import get_engine_session
from firewoes.lib import hash
import firewoes.lib.orm as fhm

from firehose.model import Analysis

from iterative_uniquify import uniquify as iterative_uniquify

import unittest
from glob import glob
import os

testsdir = os.path.dirname(os.path.abspath(__file__))
metadata = fhm.metadata

class FirewoesHashTestCase(unittest.TestCase):
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
        def get_analysis(path):
            with open(path) as xml_file:
                return Analysis.from_xml(xml_file)
        # we fill firewoes_test with our testing data:
        xml_files = glob(testsdir + "/data/*.xml")

        firehoses = [get_analysis(x) for x in xml_files]

        self.firehoses = [hash.idify(f)[0] for f in firehoses]

        self.engine, self.session = get_engine_session(app.config['DATABASE_URI'], echo=False)
#        self.engine2, self.session2 = get_engine_session('postgresql://clemux:password@localhost:5432/iterative_uniquify')

        metadata.drop_all(bind=self.engine)
        metadata.create_all(bind=self.engine)

#        metadata.drop_all(bind=self.engine2)
#        metadata.create_all(bind=self.engine2)

        self.__class__.app = app.test_client()
        self.__class__.config = app.config

    def tearDown(self):
        pass

    def test_iterative_uniquify(self):
        f = self.firehoses[4]

        print('Running iterative uniquify')
        with self.session2.no_autoflush:
            u = iterative_uniquify(self.session2, f)

        self.session2.merge(u)
        self.session2.commit()

#        print('Running recursive uniquify')
#        u_r = hash.uniquify(self.session, f)
#        self.session.merge(u_r)
#        self.session.commit()
#        self.session.expunge_all()
#        self.session.close()
#        print('Done.')

#        assert u == u_r

if __name__ == '__main__':
    unittest.main()
