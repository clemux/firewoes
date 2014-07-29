from firewoes.lib import hash
from firehose.model import _string_type


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
        self.counter() # DEBUG

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

    def save(self, session, save=True):
#        q = session.query(self.obj.__class__).filter(self.obj.__class__.id == self.obj.id)
#        if q.count() > 0:
#            print("----- Saving element already in DB")
        session.add(self.obj)
        session._unique_cache[self.key] = self.obj

    def process_child(self, child, session, save=True):
        if save:
            child.save(session)
        setattr(self.obj, child.attr_name, child.obj)

    def query(self, session):
        if not self.queried:
            class_, id_ = self.obj.__class__, self.obj.id
            res = (session.query(class_)
                   .filter(class_.id == id_).first())
            self.queried = True
            if res is not None:
                self.obj = res
                self.in_db = True
            return self.in_db
        else:
            return self.in_db

    @property
    def key(self):
        return self.obj.__class__, self.obj.id

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

    @property
    def key(self):
        return None

    @staticmethod
    def counter():
        pass

    def __repr__(self):
        return '<ListNode: %s>' % self.attr_name


def uniquify(session, obj):
    current = Node(obj, None, None, None)
#    depth = 0 # DEBUG

    cache = getattr(session, '_unique_cache', None)
    if cache is None:
        session._unique_cache = cache = {}

    while True:
#        print "%s (depth: %d)" % (current, depth) # DEBUG
        print("%d/%d" % (len(session.new), Node.count))


        if cache.get(current.key, None):
            print('Object in cache.')

        if (cache.get(current.key, None) is None and not current.query(session)):
            if not current.children_filled:
                current.fill_children()

#            print "children (%d):" % len(current.children) # DEBUG

            if len(current.children) > 0:
#                pprint.pprint(current.children) # DEBUG
                current = current.children.pop()
#                depth += 1 # DEBUG

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
            current.obj = cache[current.key]
            current.parent.process_child(current, session)
            if len(current.siblings) > 0:
                current = current.siblings.pop()
            else:
                current = current.parent
#            depth -= 1 # DEBUG
