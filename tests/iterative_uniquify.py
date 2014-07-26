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

    def save(self, session, save=True):
#        q = session.query(self.obj.__class__).filter(self.obj.__class__.id == self.obj.id)
#        if q.count() > 0:
#            print("----- Saving element already in DB")
        if len(self.obj.__class__.id.info) > 0:
            print(self.obj.__class__.id.info)

        if save:
            session.add(self.obj)

    def process_child(self, child, session, save=True):
        child.save(session)

        setattr(self.obj, child.attr_name, child.obj)

    def query(self, session):
        # if not self.queried:
        if True:
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


def uniquify(session, obj):
    current = Node(obj, None, None, None)
#    depth = 0 # DEBUG

    while True:
##        print "%s (depth: %d)" % (current, depth) # DEBUG

        if not current.query(session) and not current.obj in session.new:
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
            current.parent.process_child(current, session, False)
            if len(current.siblings) > 0:
                current = current.siblings.pop()
            else:
                current = current.parent
#            depth -= 1 # DEBUG
