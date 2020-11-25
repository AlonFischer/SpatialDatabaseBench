"""
Mysql benchmark classes
"""
from mysqlutils.mysqladapter import MySQLAdapter
from gdal.gdaldockerwrapper import GdalDockerWrapper
from benchmark.mysql_benchmark import MysqlBenchmark
import docker
import logging

DATABASE_NAME = "SpatialDatasets"


def create_mysql_adapter():
    return MySQLAdapter('root', 'root-password')


class LoadAirspaces(MysqlBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Load Airspaces"
    _table_name = "airspaces"

    def __init__(self, with_index=True):
        super().__init__(create_mysql_adapter(), LoadAirspaces._title)
        docker_client = docker.from_env()
        self.gdal_docker_wrapper = GdalDockerWrapper(docker_client)
        self.with_index = with_index

    def execute(self):
        self.gdal_docker_wrapper.import_to_mysql(
            "airspace/Class_Airspace.shp", LoadAirspaces._table_name, create_spatial_index=self.with_index)

    def cleanup(self):
        self.adapter.execute(
            f"DROP TABLE {DATABASE_NAME}.{LoadAirspaces._table_name}")

class LoadAirports(MysqlBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Load Airports"
    _table_name = "airports"

    def __init__(self, with_index=True):
        super().__init__(create_mysql_adapter(), LoadAirports._title)
        docker_client = docker.from_env()
        self.gdal_docker_wrapper = GdalDockerWrapper(docker_client)
        self.with_index = with_index

    def execute(self):
        self.gdal_docker_wrapper.import_to_mysql(
            "airports/Airports.shp", LoadAirports._table_name, create_spatial_index=self.with_index)

    def cleanup(self):
        self.adapter.execute(
            f"DROP TABLE {DATABASE_NAME}.{LoadAirports._table_name}")

class LoadRoutes(MysqlBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Load Routes"
    _table_name = "routes"

    def __init__(self, with_index=True):
        super().__init__(create_mysql_adapter(), LoadRoutes._title)
        docker_client = docker.from_env()
        self.gdal_docker_wrapper = GdalDockerWrapper(docker_client)
        self.with_index = with_index

    def execute(self):
        self.gdal_docker_wrapper.import_to_mysql(
            "routes/ATS_Route.shp", LoadRoutes._table_name, create_spatial_index=self.with_index)

    def cleanup(self):
        self.adapter.execute(
            f"DROP TABLE {DATABASE_NAME}.{LoadRoutes._table_name}")


class PolygonIntersectsPolygon(MysqlBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Polygon Intersects Polygon"

    def __init__(self, use_projected_crs=True):
        super().__init__(create_mysql_adapter(),
                         PolygonIntersectsPolygon._title, repeat_count=1)
        self.dataset_suffix = ""
        if use_projected_crs:
            self.dataset_suffix = "_3857"

    def execute(self):
        PolygonIntersectsPolygon._logger.info(
            self.adapter.execute(f"""SELECT COUNT(*)
                                    FROM {DATABASE_NAME}.airspaces{self.dataset_suffix} AS1, {DATABASE_NAME}.airspaces{self.dataset_suffix} AS2
                                    WHERE MOD(AS1.OBJECTID, 8) = 0 AND MOD(AS2.OBJECTID, 8) = 0 AND st_intersects(AS1.SHAPE, AS2.SHAPE)
                                    ;"""))


class PolygonEqualsPolygon(MysqlBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Polygon Equals Polygon"

    def __init__(self):
        super().__init__(create_mysql_adapter(), PolygonEqualsPolygon._title, repeat_count=1)

    def execute(self):
        PolygonIntersectsPolygon._logger.info(
            self.adapter.execute("""SELECT COUNT(*)
                                    FROM {DATABASE_NAME}.airspaces_projected AS1, {DATABASE_NAME}.airspaces_projected AS2
                                    WHERE MOD(AS1.OBJECTID, 8) = 0 AND MOD(AS2.OBJECTID, 8) = 0 AND st_equals(AS1.SHAPE, AS2.SHAPE)
                                    ;"""))


class LineIntersectsLine(MysqlBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Line Intersects Line"

    def __init__(self):
        super().__init__(create_mysql_adapter(), LineIntersectsLine._title, repeat_count=1)

    def execute(self):
        LineIntersectsLine._logger.info(
            self.adapter.execute("""SELECT COUNT(*)
                                    FROM {DATABASE_NAME}.routes_projected R1, {DATABASE_NAME}.routes_projected R2
                                    WHERE MOD(R1.OBJECTID, 4) = 0 AND MOD(R2.OBJECTID, 4) = 0 AND st_intersects(R1.SHAPE, R2.SHAPE)
                                    ;"""))

# class SelectStatic(mysql_benchmark.MysqlBenchmark):
#     def __init__(self):
#         super().__init__()
#         self.title = "SELECT 1"

#     def execute(self):
#         repeat = self.repeat()
#         db = self.connection()
#         stmt = db.cursor()

#         for i in range(0, repeat):
#             stmt.execute("SELECT 1")
#         stmt.close()


# class ConnectSelectStatic(mysql_benchmark.MysqlBenchmark):
#     def __init__(self):
#         super().__init__()
#         self.title = "CONNECT; SELECT 1; CLOSE"

#     def execute(self):
#         repeat = self.repeat()

#         for i in range(0, repeat):
#             db = self.newConnection()
#             stmt = db.cursor()
#             stmt.execute("SELECT 1")
#             stmt.close()
#             db.close()


# class DropCreate(mysql_benchmark.MysqlBenchmark):
#     def __init__(self):
#         super().__init__()
#         self.title = "DROP table; CREATE table"
#         self.repeat_factor = 1
#         with open('config/mysql-create-table.sql') as f:
#             self.create_sql = f.read()

#     def execute(self):
#         repeat = self.repeat()
#         db = self.connection()
#         drop_stmt = db.cursor()
#         create_stmt = db.cursor()

#         for i in range(0, repeat):
#             try:
#                 drop_stmt.execute("DROP TABLE test_simple")
#                 db.commit()
#             except:
#                 pass

#             create_stmt.execute(self.create_sql)
#             db.commit()

#         drop_stmt.close()
#         create_stmt.close()


# class Insert(mysql_benchmark.MysqlBenchmark):
#     def __init__(self):
#         super().__init__()
#         self.title = "INSERT INTO table"
#         self.repeat_factor = 10

#     def execute(self):
#         repeat = self.repeat()
#         db = self.connection()
#         stmt = db.cursor()

#         for i in range(0, repeat):
#             stmt.execute("INSERT INTO test_simple (value) VALUES (1)")

#         stmt.close()
#         db.commit()


# class SelectTable(mysql_benchmark.MysqlBenchmark):
#     def __init__(self, count):
#         super().__init__()
#         self.title = "SELECT * FROM table LIMIT " + str(count)
#         self.sql = "SELECT * FROM test_simple LIMIT " + str(count)
#         self.expect_count = count

#     def execute(self):
#         repeat = self.repeat()
#         db = self.connection()
#         stmt = db.cursor()

#         for i in range(0, repeat):
#             stmt.execute(self.sql)
#             row_count = 0
#             for row in stmt:
#                 row_count = row_count + 1

#             if row_count != self.expect_count:
#                 raise Exception('Invalid result count:' + str(row_count))

#         stmt.close()


# def create():
#     s = SelectStatic()
#     return [DoStatic(), SelectStatic(), ConnectSelectStatic(), DropCreate(), Insert(),
#             SelectTable(1), SelectTable(10), SelectTable(100), SelectTable(500), SelectTable(1000)]
