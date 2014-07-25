from firewoes.web.app import app
from firewoes.lib.dbutils import get_engine_session
import firewoes.lib.orm as fhm
from firewoes.lib import hash
from firehose.model import Analysis

from iterative_uniquify import uniquify as iterative_uniquify

from glob import glob
import os
import timeit


testsdir = os.path.dirname(os.path.abspath(__file__))
metadata = fhm.metadata


def get_analysis(path):
    with open(path) as xml_file:
        return Analysis.from_xml(xml_file)



def run():
    xml_files = glob(testsdir + "/data/*.xml")
    firehoses = [get_analysis(x) for x in xml_files]
    firehoses = [hash.idify(f)[0] for f in firehoses]
    engine, session = get_engine_session(app.config['DATABASE_URI'], echo=False)
    metadata.drop_all(bind=engine)
    metadata.create_all(bind=engine)

    f = firehoses[4]
    with session.no_autoflush:
        iterative_uniquify(session, f)

def run_recursive():
    xml_files = glob(testsdir + "/data/*.xml")
    firehoses = [get_analysis(x) for x in xml_files]
    firehoses = [hash.idify(f)[0] for f in firehoses]
    engine, session = get_engine_session(app.config['DATABASE_URI'], echo=False)

    f = firehoses[4]
    hash.uniquify(session, f)




if __name__ == '__main__':
    import sys
    if (len(sys.argv) > 1):
        arg = sys.argv[1]
    else:
        arg = 'iterative-notimeit'

    engine, session = get_engine_session(app.config['DATABASE_URI'], echo=False)
    metadata.drop_all(bind=engine)
    metadata.create_all(bind=engine)
    if arg == "iterative":
        print(timeit.timeit('run()', setup='from __main__ import run', number=1))
    elif arg == "recursive":
        print(timeit.timeit('run_recursive()', setup='from __main__ import run_recursive', number=1))
    elif arg =='iterative-notimeit':
        run()
