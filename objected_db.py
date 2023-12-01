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
        if params:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
        else:
            with self.connection.cursor() as cursor:
                cursor.execute(query)

    def fetch(self, query):
        with self.connection.cursor() as cursor:
            return cursor.fetchall(query)


class Query:
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.conditions = []
        self.values = []
        self.columns = []
        self.operation = None
        self.query = None

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
            query = "select {fields} from {table} where {condition}".format(
                fields=", ".join(self.columns),
                table=self.table_name,
                condition=" ".join(self.conditions),
            )
        elif self.operation == 'insert':
            query = "insert into {table} ({fields}) values ({values})".format(
                fields=", ".join(self.columns),
                table=self.table_name,
                values=", ".join(self.values)
            )
        elif self.operation == 'update':
            query = "update {table} set {fields} where {values}".format(
                fields=" ".join(self.values),
                table=self.table_name,
                values=" ".join(self.values)
            )
        elif self.operation == 'delete':
            query = "delete from {table} where {condition}".format(
                table=self.table_name,
                condition=" ".join(self.conditions)
            )
        db = DBHandler(connection_parameters)
        if self.operation == 'select':
            return db.fetch(query)
        else:
            db.exec(query)
            return "query executed"


# q = Query(table_name='users')
# q.select('id', 'name')
# q.where('id', '=', '1')
# print(q.build())

# q = Query(table_name='users')
# q.insert(columns=['id', 'name'], values=['1', 'ehsan'])
# print(q.build())

# q = Query(table_name='users')
# q.update(['name', '=', 'ehsan'])
# q.where('id', '=', '1')
# print(q.build())

q = Query(table_name='users')
q.delete()
q.where('id', '=', '1')
print(q.build())

