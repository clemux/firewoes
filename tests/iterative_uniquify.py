from firewoes.lib import hash
from firehose.model import _string_type

# import pprint # DEBUG



# performance will probably not be great with such objects,
# but they're easier to manipulate for now

# TODO: use weakref for `parent` and `siblings`
# TODO: profile memory use (guppy)
# TODO: use a lighter data structure



class Node(object):
    count = 0 # DEBUG

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
        self.queried = False
        self.in_db = False
        self.counter() # DEBUG

    @staticmethod
    def counter(): # DEBUG
        Node.count += 1

    def fill_children(self):
        # from original implementation
        for (attr_name, attr) in hash.get_attrs(self.obj):
            if isinstance(attr, list):
                self.children.append(ListNode(attr, self, attr_name, self.children))
            elif (type(attr) not in (int, float, str, _string_type)
                  and attr is not None):
                self.children.append(Node(attr, self, attr_name, self.children))
        self.children_filled = True

    def save(self, session):
        """
        Adds the current object in the session and in the cache
        """
        if self.key in session._unique_cache: # DEBUG
            print('Saving object already in cache') # DEBUG
        session.add(self.obj)
        session._unique_cache[self.key] = self.obj

    def process_child(self, child):
        """
        Update an attribute with the given child's firehose object.

        Done in the node's parent instead of itself,
        because ListNode's children are not attributes.
        """
        setattr(self.obj, child.attr_name, child.obj)

    def query(self, session):
        """
        If the object is not in the DB, it won't be in the DB
        later, since we disable session autoflush.
        """
        if not self.queried:
            class_, id_ = self.obj.__class__, self.obj.id
            res = (session.query(class_)
                   .filter(class_.id == id_).first())
            self.queried = True
            if res is not None:
                self.obj = res # let's use the object already in the DB
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
    """
    Some attributes are actually list of firehose objets.
    A ListNode represents such a list, faking a normal node.
    `obj` is a list of attributes.
    """

    def fill_children(self):
        self.children = [Node(item, self, None, self.children) for item in self.obj]
        self.children_filled = True

    def save(self, session):
        """
        I'm not an actual firehose objet, so I can't be
        added to the session or the cache.
        """

    def process_child(self, child):
        """
        I'm a not an actual firehose objet, so there's nothing to do here.
        """
        pass

    def query(self, session):
        """
        I can't be in the DB, since I don't exist.
        """
        return False

    @property
    def key(self):
        """ I'm not a firehose element, so I have no key """
        return None

    @staticmethod
    def counter():
        # DEBUG
        pass

    def __repr__(self):
        return '<ListNode: %s>' % self.attr_name


def uniquify(session, obj):
    # root node (`Analysis` object)
    # so no parent, and no attribute name, and no siblings
    current = Node(obj, None, None, None)
#    depth = 0 # DEBUG

    session._unique_cache = cache = {}

    while True:
#        print "%s (depth: %d)" % (current, depth) # DEBUG

        # DEBUG
        print("%d session/%d cache/%d total" % (len(session.new),
                                                len(cache), Node.count))

        if current.query(session): # DEBUG
            # This should not happen with a pristine DB,
            # and will make the check below fail
            # (see FIXME comment below).
            print('!!! Object in DB !!!') # DEBUG


#        if (cache.get(current.key, None) is None) and not current.query(session)):
        # FIXME: add the query check again (useless for now, since the DB is empty)
        if (cache.get(current.key, None) is None):
            if not current.children_filled:
                current.fill_children()

#            print "children (%d):" % len(current.children) # DEBUG

            # (1) if children, go back to (1) with first child
            # (2) process self
            # (3) if siblings, go back to (1) with first sibling

            if len(current.children) > 0:
#                pprint.pprint(current.children) # DEBUG
                current = current.children.pop()
#                depth += 1 # DEBUG

            elif current.parent is not None:
#                print('-- no child, processing current (depth: %d)' % depth) # DEBUG
                current.parent.process_child(current)
                current.save(session)
#                print('-- moving up') # DEBUG
                if len(current.siblings) > 0:
                    current = current.siblings.pop()
                else:
                    current = current.parent
#                    depth -= 1 # DEBUG

            else:
                session.add(current.obj)
                print(' **** Tree processed, returning.') # DEBUG
                return current.obj

        elif current.parent == None:
#            print(' **** Object exists, returning.') # DEBUG
            return current.obj

        else:
#            print("-- Object exists, moving up (depth: %d)" % depth) # DEBUG
            current.obj = cache[current.key]
            current.parent.process_child(current)
            if len(current.siblings) > 0:
                current = current.siblings.pop()
            else:
                current = current.parent
#            depth -= 1 # DEBUG
