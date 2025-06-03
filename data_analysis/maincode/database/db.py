import MySQLdb
import time

class DB:
    def connect(self):
        self.conn = MySQLdb.connect(host='localhost', user='root', passwd='1234', db='Apartment-transaction', charset='UTF8')

    def execute(self, query: str, retry=1):
        if not query:
            return

        count = 0
        while count < retry:
            try:
                cur = self.conn.cursor()
                cur.execute(query)
                return cur.fetchall()
            except Exception as ex:
                time.sleep(0.3)

                count += 1
                if count >= retry:
                    raise Exception(ex)

    def disconnect(self):
        self.conn.commit()
        self.conn.close()

    def handle(func):
        def wrapper(self, *args, **kwargs):
            self.connect()
            row = self.execute(func(*args, **kwargs))
            self.disconnect()
            return row
        return wrapper

class Query(DB):
    @DB.handle
    def create(table: str, options: list):
        return f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(options)});"

    @DB.handle
    def select(table: str, columns: list, where: list=None):
        if not where:
            return f"SELECT {', '.join(columns)} FROM {table};"
        return f"SELECT {', '.join(columns)} FROM {table} WHERE {' AND '.join(where)};"

    @DB.handle
    def insert(table: str, columns: list, values: list):
        return f"INSERT INTO {table}({', '.join(columns)}) VALUES({', '.join(values)});"

    @DB.handle
    def delete(table:str, where: list):
        return f"DELETE FROM {table} WHERE {' AND '.join(where)};"
