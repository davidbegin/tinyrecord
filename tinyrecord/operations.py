import abc


def null_query(x):
    """
    Returns false regardless of the document
    passed to the function.
    """
    return False


class Operation(abc.ABC):
    @abc.abstractmethod
    def perform(self):
        """
        An operation represents a single, atomic
        sequence of things to do to in-memory data.
        Every operation must implement the abstract
        ``perform`` method.
        """
        return


class InsertMultiple(Operation):
    """
    Insert multiple records *iterable* into the
    database.

    :param iterable: The iterable of elements to
        be inserted into the DB.
    """
    def __init__(self, iterable):
        self.iterable = iterable

    def perform(self, table):
        doc_id = max(table) if table else 0
        for element in self.iterable:
            doc_id += 1
            table[doc_id] = element


class UpdateCallable(Operation):
    """
    Mutate each of the records with a given
    *function* for all records that match a
    certain *query*.

    :param fields: The fields to update.
    """
    def __init__(self, function, query=null_query, doc_ids=[]):
        self.function = function
        self.query = query
        self.doc_ids = set(doc_ids)

    def perform(self, table):
        for key in table:
            value = table[key]
            if key in self.doc_ids or self.query(value):
                self.function(value)


class Remove(Operation):
    """
    Remove documents from the DB matching
    the given *query*.

    :param query: The query to remove.
    """
    def __init__(self, query=null_query, doc_ids=[]):
        self.query = query
        self.doc_ids = set(doc_ids)

    def perform(self, table):
        for key in list(table):
            if key in self.doc_ids or self.query(table[key]):
                del table[key]
