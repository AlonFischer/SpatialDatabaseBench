import mysql.connector
import psycopg2


class PostgisAdapter:
    def __init__(self, user, password, host="127.0.0.1", port="5432", dbname='spatialdatasets', persist = False):
        self.connection = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=dbname,
        )
        self._persist = persist

    def __del__(self):
        try:

            self.connection.close()
        except:
            pass

    def execute(self, query):
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
        except Exception as e:
            print("Query exception:")
            print(f"\tQuery: {query}")
            print(f"\tException: {e}")
            raise e
        if self._persist:
            self.connection.commit()
        else:
            self.connection.rollback()
        if cursor.description != None:
            return cursor.fetchall()
        else:
            return None

    def execute_nontransaction(self, query):
        old_isolation_level = self.connection.isolation_level
        self.connection.set_isolation_level(0)
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
        except Exception as e:
            print("Query exception:")
            print(f"\tQuery: {query}")
            print(f"\tException: {e}")
            raise e
        finally:
            self.connection.set_isolation_level(old_isolation_level)

    def get_schemas(self):
        return [tuple[0] for tuple in self.execute("SELECT datname FROM pg_database;")]
