import psycopg2 as postgres
from psycopg2 import sql

connection_parameters = {
    'dbname   ': 'test_db',
    'user     ': 'postgres',
    'password ': 123,
    'host     ': 'localhost',
    'port     ': 5432}


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
        self.columns.append(*columns)
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
            query = sql.SQL("select {field} from {table} where {pkey} = %s").format(
                field=sql.Identifier('my_name'),
                table=sql.Identifier('some_table'),
                pkey=sql.Identifier('id')
            )


q = Query(table_name='users')
result = q.select(['id', 'name'])

db_handler = DBHandler(connection_parameters)
