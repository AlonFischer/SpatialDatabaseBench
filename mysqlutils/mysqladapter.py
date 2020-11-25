import mysql.connector


class MySQLAdapter:
    def __init__(self, user, password, host="127.0.0.1", port="3306"):
        self.connection = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            connection_timeout=10
        )

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

    def get_schemas(self):
        return [tuple[0] for tuple in self.execute("SHOW SCHEMAS")]
