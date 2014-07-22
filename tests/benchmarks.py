from firewoes.web.app import app
from firewoes.lib.dbutils import get_engine_session
import firewoes.lib.orm as fhm
from firewoes.lib import hash
from firehose.model import Analysis, _string_type

from glob import glob
import os
import timeit


testsdir = os.path.dirname(os.path.abspath(__file__))
metadata = fhm.metadata


def get_analysis(path):
    with open(path) as xml_file:
        return Analysis.from_xml(xml_file)


class Node(object):
    count = 0 # DEBUG

    def __init__(self, obj, parent, attr_name, siblings=None):
        self.obj = obj
        self.parent = parent
        self.attr_name = attr_name
        self.children = list()
        if siblings is not None:
            self.siblings = siblings
        self.children_filled = False
        self.queried = False
        self.in_db = False
        Node.counter() # DEBUG

    @staticmethod
    def counter(): # DEBUG
        Node.count += 1

    def fill_children(self):
        for (attr_name, attr) in hash.get_attrs(self.obj):
            if isinstance(attr, list):
                self.children.append(ListNode(attr, self, attr_name, self.children))
            elif (type(attr) not in (int, float, str, _string_type)
                  and attr is not None):
                self.children.append(Node(attr, self, attr_name, self.children))
        self.children_filled = True

    def save(self, session):
        session.add(self.obj)

    def process_child(self, child, session):
        child.save(session)
        setattr(self.obj, child.attr_name, child.obj)

    def query(self, session):
        if not self.queried:
            class_, id_ = self.obj.__class__, self.obj.id
            res = (session.query(class_)
                   .filter(class_.id == id_).first())
            self.queried = True
            self.in_db = res != None
            return self.in_db
        else:
            return self.in_db



    def __repr__(self):
        return '<Node: %s/%s>' % (self.obj.__class__, self.obj.id)

class ListNode(Node):
    def fill_children(self):
        self.children = [Node(item, self, None, self.children) for item in self.obj]
        self.children_filled = True

    def save(self, session):
        pass

    def process_child(self, child, session):
        pass

    def query(self, session):
        return False

    def __repr__(self):
        return '<ListNode: %s>' % self.attr_name

def iterative_uniquify(session, obj):
    current = Node(obj, None, None, None)
#    depth = 0 # DEBUG
    while True:
#        print "%s (depth: %d)" % (current, depth) # DEBUG

        print Node.count
        if not current.query(session):
            if not current.children_filled:
                current.fill_children()

#            print "children (%d):" % len(current.children) # DEBUG

            if len(current.children) > 0:
#                pprint.pprint(current.children) # qDEBUG
                current = current.children.pop()
#                depth += 1 # DEBUG
                continue

            elif current.parent is not None:
#                print('-- no child, processing current (depth: %d)' % depth) # DEBUG
                current.parent.process_child(current, session)
#                print('-- moving up') # DEBUG
                if len(current.siblings) > 0:
                    current = current.siblings.pop()
                else:
                    current = current.parent

#                depth -= 1 # DEBUG

            else:
                session.add(current.obj)
#                print(' **** Tree processed, returning.') # DEBUG
                return current.obj

        elif current.parent == None:
#            print(' **** Object exists, returning.') # DEBUG
            return current.obj

        else:
#            print("-- Object exists, moving up (depth: %d)" % depth) # DEBUG
            current.parent.process_child(current, session)
            if len(current.siblings) > 0:
                current = current.siblings.pop()
            else:
                current = current.parent
#            depth -= 1 # DEBUG

def run():
    xml_files = glob(testsdir + "/data/*.xml")
    firehoses = [get_analysis(x) for x in xml_files]
    firehoses = [hash.idify(f)[0] for f in firehoses]
    engine, session = get_engine_session(app.config['DATABASE_URI'], echo=False)

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
