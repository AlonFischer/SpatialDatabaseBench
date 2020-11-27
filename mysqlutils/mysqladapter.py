import time
import mysql.connector


class MySQLAdapter:
    def __init__(self, user, password, host="127.0.0.1", port="3306"):
        attempt = 0
        while True:
            try:
                self.connection = mysql.connector.connect(
                    host=host,
                    port=port,
                    user=user,
                    password=password,
                    connection_timeout=10
                )
                break
            except Exception as e:
                attempt += 1
                if attempt == 25:
                    raise e
                time.sleep(1)

    def __del__(self):
        try:
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

    def commit(self):
        self.connection.commit()

    def get_schemas(self):
        return [tuple[0] for tuple in self.execute("SHOW SCHEMAS")]
