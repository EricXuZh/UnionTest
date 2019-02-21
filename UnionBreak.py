from UnionAbstract import UnionAbstract
import random as rand
import re


class Breaker(UnionAbstract):
    def rowSplinter(self, name, number_of_tables, values_per):
        self.ensure_in_db(name)

        self.c.execute("select count (*) from %s" % (name))
        rows = self.c.fetchall()[0][0]
        if rows < values_per:
            values_per = rows

        shift = len(self.get_tables_of_pattern(re.compile(r"r_split_{}".format(name))))

        for i in range(0, number_of_tables):
            chose_rows = rand.sample(range(1, rows), values_per)
            temp_db_name = "r_split_%s_%d" % (name, shift + i)
            for cr in range(len(chose_rows)):
                if cr == 0:
                    self.c.execute(
                        (
                            "create table {} as select" "* from {} limit 1 offset {}"
                        ).format(temp_db_name, name, chose_rows[cr])
                    )
                else:
                    self.c.execute(
                        (
                            "insert into {} select * from {} limit 1"
                            " offset {}".format(temp_db_name, name, chose_rows[cr])
                        )
                    )
                self.conn.commit()

    def __init__(self, path_to_db):
        super().__init__(path_to_db)

    def columnSplinter(self, name, number_partitions, shared_columns):
        self.ensure_in_db(name)
        cnames = self.column_names_of_table(name)
        if len(cnames) - shared_columns / number_partitions < 1:
            raise ValueError(("Not Enough Columns to" "properly split column wise"))
            exit()

        chosen = rand.sample(cnames, shared_columns)
        columnsLeft = list(filter(lambda x: x not in chosen, cnames))
        overflow = len(columnsLeft) % number_partitions
        partitions_per = [
            (int)(len(columnsLeft) / number_partitions)
            for i in range(0, number_partitions)
        ]

        for i in range(0, overflow):
            partitions_per[i] += 1

        sep_tables = []
        for p in partitions_per:
            distr_col = rand.sample(columnsLeft, p)
            sep_tables.append(distr_col)
            columnsLeft = list(filter(lambda x: x not in distr_col, columnsLeft))

        shift = len(self.get_tables_of_pattern(re.compile(r"c_split_{}".format(name))))

        for cols in sep_tables:
            temp_arr = chosen.copy()
            temp_arr.extend(cols)
            temp_arr = self.bracketize(temp_arr)
            rand.shuffle(temp_arr)
            query = ", ".join(temp_arr)
            self.c.execute(
                "create table c_split_{}_{} as select {} from {}".format(
                    name, shift, query, name
                )
            )
            self.conn.commit()
            shift += 1

    def flush_created_tables(self, type="row"):
        tables = []
        if type == "row":
            tables = self.get_tables_of_type(re.compile(r"r_split_*"))
        elif type == "col":
            tables = self.get_tables_of_type(re.compile(r"c_split_*"))

        for table in tables:
            self.c.execute("drop table {}".format(table))
        self.conn.commit()

    def shuffle_cols(self, table_name, delete=True):
        self.ensure_in_db(table_name)
        cnames = self.column_names_of_table(table_name)
        cnames = self.bracketize(cnames)
        rand.shuffle(cnames)

        self.c.execute(
            "create table shuffle_{} as select {} from {}".format(
                table_name, ", ".join(cnames), table_name
            )
        )

        self.conn.commit()

        if delete:
            self.c.execute("drop table {}".format(table_name))
            self.conn.commit()
