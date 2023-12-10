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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.connection.close()

    def exec(self, query, params=None):
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)

    def fetch(self, query):
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()

class Query:
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.conditions = []
        self.values = []
        self.columns = []
        self.operation = None

    def select(self, *columns):
        self.columns.extend(columns)
        self.operation = 'select'
        return self

    def insert(self, **kwargs):
        self.columns.extend(kwargs['columns'])
        self.values.extend(kwargs['values'])
        self.operation = 'insert'
        return self

    def update(self, values):
        self.values.extend(values)
        self.operation = 'update'
        return self

    def delete(self):
        self.operation = 'delete'
        return self

    def where(self, *conditions):
        self.conditions.extend(conditions)
        return self

    def build(self):
        query = None
        if self.operation == 'select':
            fields = [sql.Identifier(column) for column in self.columns]
            condition = sql.SQL(" AND ").join([sql.Identifier(cond) for cond in self.conditions])
            query = sql.SQL("SELECT {} FROM {} WHERE {}").format(
                sql.SQL(", ").join(fields),
                sql.Identifier(self.table_name),
                condition
            )
        elif self.operation == 'insert':
            fields = [sql.Identifier(column) for column in self.columns]
            values = [sql.Literal(value) for value in self.values]
            query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(self.table_name),
                sql.SQL(", ").join(fields),
                sql.SQL(", ").join(values)
            )
        elif self.operation == 'update':
            fields = sql.SQL(", ").join([sql.Identifier(value) for value in self.values])
            condition = sql.SQL(" AND ").join([sql.Identifier(cond) for cond in self.conditions])
            query = sql.SQL("UPDATE {} SET {} WHERE {}").format(
                sql.Identifier(self.table_name),
                fields,
                condition
            )
        elif self.operation == 'delete':
            condition = sql.SQL(" AND ").join([sql.Identifier(cond) for cond in self.conditions])
            query = sql.SQL("DELETE FROM {} WHERE {}").format(
                sql.Identifier(self.table_name),
                condition
            )
        with DBHandler(connection_parameters) as db:
            if self.operation == 'select':
                return db.fetch(query)
            else:
                db.exec(query)
                return "Query executed"

# نمونه استفاده:
q = Query(table_name='users')
q.delete()
q.where('id', '=', '1')
print(q.build())
