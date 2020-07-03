class Changeset:
    """
    A changeset represents a series of changes that
    can be applied to the *database*.

    :param database: The TinyDB table.
    """

    def __init__(self, database):
        self.db = database
        self.record = []

    def execute(self):
        """
        Execute the changeset, applying every
        operation on the database. Note that this
        function is not idempotent, if you call
        it again and again it will be executed
        many times.
        """
        def updater(table: dict):
            for operation in self.record:
                operation.perform(table)

        self.db._update_table(updater)
        self.db.clear_cache()

    def append(self, change):
        """
        Append a *change* to the internal record
        of operations.

        :param change: The change to append.
        """
        self.record.append(change)
