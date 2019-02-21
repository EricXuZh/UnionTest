import sqlite3 as sq
from abc import ABC, abstractmethod


class UnionAbstract(ABC):

    @abstractmethod
    def __init__(self, path_to_db):
        self.conn = sq.connect(path_to_db)
        self.c = self.conn.cursor()
        self.keywords = set()
        self.add_kw()
        self.c.execute(("select name from sqlite_master "
                        "where type = 'table';"))
        self.names = [sublist[0] for sublist in self.c.fetchall()]

    def column_names_of_table(self, name):
        self.c.execute("pragma table_info('{}')".format(name))
        return [t[1] for t in self.c.fetchall()]

    def retrieve_table_names(self):
        return self.names

    def add_kw(self):
        self.keywords.add("index")

    def close(self):
        self.conn.commit()
        self.conn.close()

    def get_tables_of_pattern(self, pattern):
        self.c.execute(("select name from sqlite_master "
                        "where type = 'table';"))
        tables = [i[0] for i in self.c.fetchall()]
        return list(filter(pattern.match, tables))

    def ensure_in_db(self, name):
        if name not in self.names:
            raise ValueError("Given table name not found in the database.")

    def bracketize(self, str_array):
        return list(map(lambda x: "[" + x + "]" if x in self.keywords
                        else x, str_array))
