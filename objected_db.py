import psycopg2 as postgres
from psycopg2 import sql

connection_parameters = {
    'dbname   ': 'test_db',
    'user     ': 'postgres',
    'password ': 1,
    'host     ': 'localhost',
    'port     ': 5432
}


class DBHandler:
    def __init__(self, connection_parameters) -> None:
        self.connection = postgres.connect(**connection_parameters)

    def exec(self, query, params=None):
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)


class Query:
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.conditions = []
        self.values = []
        self.columns = []

    def select(self, *columns):
        self.columns.extend(columns)
        return self

    def create(self, values):
        self.values.append(values)
        return self

    def update(self, values):
        self.values.append(values)
        return self

    def delete(self):
        return self

    def where(self, conditions):
        self.conditions.append(conditions)
        return self

    def build(self):
        query = None
        if self.columns:
            query = "select {field} from {table} where {pkey}".format(
                field=", ".join(self.columns),
                table=self.table_name,
                pkey='id',
            )

            return query


# db_handler = DBHandler(connection_parameters)
q = Query(table_name='users')
q.select('*')
q.where('ww')
result = q.build()
print(result)
