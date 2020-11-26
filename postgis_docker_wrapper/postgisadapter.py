import mysql.connector
import psycopg2


class PostgisAdapter:
    def __init__(self, user, password, host="127.0.0.1", port="5432"):
        self.connection = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
        )

    def __del__(self):
        try:
            self.connection.rollback()
            self.connection.close()
        except:
            pass

    def execute(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        if cursor.description != None:
            return cursor.fetchall()
        else:
            return None

    def get_schemas(self):
        return [tuple[0] for tuple in self.execute("SHOW SCHEMAS")]
