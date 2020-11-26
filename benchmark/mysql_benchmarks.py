"""
Mysql benchmark classes
"""
from mysqlutils.mysqladapter import MySQLAdapter
from gdal.gdaldockerwrapper import GdalDockerWrapper
from benchmark.mysql_benchmark import MysqlBenchmark
from util.coordinate_transform import transform_4326_to_3857
from util.create_geometry import create_polygon
import docker
import logging

DATABASE_NAME = "SpatialDatasets"
GEORGIA_BOUNDING_BOX = [(30.3575, -85.6082), (34.9996, -85.6082),
                        (34.9996, -80.696), (30.3575, -80.696), (30.3575, -85.6082)]
GEORGIA_BB_4326 = create_polygon(GEORGIA_BOUNDING_BOX)
GEORGIA_BB_3857 = create_polygon(
    [transform_4326_to_3857(point) for point in GEORGIA_BOUNDING_BOX])


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


class PointEqualsPoint(MysqlBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Point Equals Point"

    def __init__(self, use_projected_crs=True, subsampling_factor=1):
        super().__init__(create_mysql_adapter(),
                         PointEqualsPoint._title, repeat_count=1)
        self.dataset_suffix = ""
        if use_projected_crs:
            self.dataset_suffix = "_3857"
        self.subsampling_condition = ""
        if subsampling_factor > 1:
            self.subsampling_condition = f"MOD(A1.OBJECTID, {subsampling_factor}) = 0 AND MOD(A2.OBJECTID, {subsampling_factor}) = 0 AND"

    def execute(self):
        cmd = f"""SELECT A1.OBJECTID, A2.OBJECTID
                FROM {DATABASE_NAME}.airports{self.dataset_suffix} A1, {DATABASE_NAME}.airports{self.dataset_suffix} A2
                WHERE {self.subsampling_condition} st_equals(A1.SHAPE, A2.SHAPE)
                ;"""
        PointEqualsPoint._logger.info(f"Query: {cmd}")
        return self.adapter.execute(cmd)


class PointIntersectsLine(MysqlBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Point Intersects Line"

    def __init__(self, use_projected_crs=True, subsampling_factor=1):
        super().__init__(create_mysql_adapter(),
                         PointIntersectsLine._title, repeat_count=1)
        self.dataset_suffix = ""
        if use_projected_crs:
            self.dataset_suffix = "_3857"
        self.subsampling_condition = ""
        if subsampling_factor > 1:
            self.subsampling_condition = f"MOD(A1.OBJECTID, {subsampling_factor}) = 0 AND MOD(R2.OBJECTID, {subsampling_factor}) = 0 AND"

    def execute(self):
        cmd = f"""SELECT A1.OBJECTID, R2.OBJECTID
                FROM {DATABASE_NAME}.airports{self.dataset_suffix} A1, {DATABASE_NAME}.routes{self.dataset_suffix} R2
                WHERE {self.subsampling_condition} st_intersects(A1.SHAPE, R2.SHAPE)
                ;"""
        PointIntersectsLine._logger.info(f"Query: {cmd}")
        return self.adapter.execute(cmd)


class PointWithinPolygon(MysqlBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Point Within Polygon"

    def __init__(self, use_projected_crs=True, subsampling_factor=1):
        super().__init__(create_mysql_adapter(),
                         PointWithinPolygon._title, repeat_count=1)
        self.dataset_suffix = ""
        if use_projected_crs:
            self.dataset_suffix = "_3857"
        self.subsampling_condition = ""
        if subsampling_factor > 1:
            self.subsampling_condition = f"MOD(A1.OBJECTID, {subsampling_factor}) = 0 AND MOD(AS2.OBJECTID, {subsampling_factor}) = 0 AND"

    def execute(self):
        cmd = f"""SELECT A1.OBJECTID, AS2.OBJECTID
                FROM {DATABASE_NAME}.airports{self.dataset_suffix} A1, {DATABASE_NAME}.airspaces{self.dataset_suffix} AS2
                WHERE {self.subsampling_condition} st_within(A1.SHAPE, AS2.SHAPE)
                ;"""
        PointWithinPolygon._logger.info(f"Query: {cmd}")
        return self.adapter.execute(cmd)


class LineIntersectsPolygon(MysqlBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Line Intersects Polygon"

    def __init__(self, use_projected_crs=True, subsampling_factor=1):
        super().__init__(create_mysql_adapter(),
                         LineIntersectsPolygon._title, repeat_count=1)
        self.dataset_suffix = ""
        if use_projected_crs:
            self.dataset_suffix = "_3857"
        self.subsampling_condition = ""
        if subsampling_factor > 1:
            self.subsampling_condition = f"MOD(R1.OBJECTID, {subsampling_factor}) = 0 AND MOD(AS2.OBJECTID, {subsampling_factor}) = 0 AND"

    def execute(self):
        cmd = f"""SELECT R1.OBJECTID, AS2.OBJECTID
                FROM {DATABASE_NAME}.routes{self.dataset_suffix} R1, {DATABASE_NAME}.airspaces{self.dataset_suffix} AS2
                WHERE {self.subsampling_condition} st_intersects(R1.SHAPE, AS2.SHAPE)
                ;"""
        LineIntersectsPolygon._logger.info(f"Query: {cmd}")
        return self.adapter.execute(cmd)


class LineWithinPolygon(MysqlBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Line Within Polygon"

    def __init__(self, use_projected_crs=True, subsampling_factor=1):
        super().__init__(create_mysql_adapter(),
                         LineWithinPolygon._title, repeat_count=1)
        self.dataset_suffix = ""
        if use_projected_crs:
            self.dataset_suffix = "_3857"
        self.subsampling_condition = ""
        if subsampling_factor > 1:
            self.subsampling_condition = f"MOD(R1.OBJECTID, {subsampling_factor}) = 0 AND MOD(AS2.OBJECTID, {subsampling_factor}) = 0 AND"

    def execute(self):
        cmd = f"""SELECT R1.OBJECTID, AS2.OBJECTID
                FROM {DATABASE_NAME}.routes{self.dataset_suffix} R1, {DATABASE_NAME}.airspaces{self.dataset_suffix} AS2
                WHERE {self.subsampling_condition} st_within(R1.SHAPE, AS2.SHAPE)
                ;"""
        LineWithinPolygon._logger.info(f"Query: {cmd}")
        return self.adapter.execute(cmd)


class LineIntersectsLine(MysqlBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Line Intersects Line"

    def __init__(self, use_projected_crs=True, subsampling_factor=1):
        super().__init__(create_mysql_adapter(), LineIntersectsLine._title, repeat_count=1)
        self.dataset_suffix = ""
        if use_projected_crs:
            self.dataset_suffix = "_3857"
        self.subsampling_condition = ""
        if subsampling_factor > 1:
            self.subsampling_condition = f"MOD(R1.OBJECTID, {subsampling_factor}) = 0 AND MOD(R2.OBJECTID, {subsampling_factor}) = 0 AND"

    def execute(self):
        cmd = f"""SELECT R1.OBJECTID, R2.OBJECTID
                FROM {DATABASE_NAME}.routes{self.dataset_suffix} R1, {DATABASE_NAME}.routes{self.dataset_suffix} R2
                WHERE {self.subsampling_condition} st_intersects(R1.SHAPE, R2.SHAPE)
                ;"""
        LineIntersectsLine._logger.info(f"Query: {cmd}")
        return self.adapter.execute(cmd)


class PolygonEqualsPolygon(MysqlBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Polygon Equals Polygon"

    def __init__(self, use_projected_crs=True, subsampling_factor=1):
        super().__init__(create_mysql_adapter(), PolygonEqualsPolygon._title, repeat_count=1)
        self.dataset_suffix = ""
        if use_projected_crs:
            self.dataset_suffix = "_3857"
        self.subsampling_condition = ""
        if subsampling_factor > 1:
            self.subsampling_condition = f"MOD(AS1.OBJECTID, {subsampling_factor}) = 0 AND MOD(AS2.OBJECTID, {subsampling_factor}) = 0 AND"

    def execute(self):
        cmd = f"""SELECT AS1.OBJECTID, AS2.OBJECTID
                FROM {DATABASE_NAME}.airspaces{self.dataset_suffix} AS1, {DATABASE_NAME}.airspaces{self.dataset_suffix} AS2
                WHERE {self.subsampling_condition} st_equals(AS1.SHAPE, AS2.SHAPE)
                ;"""
        PolygonEqualsPolygon._logger.info(f"Query: {cmd}")
        return self.adapter.execute(cmd)


class PolygonDisjointPolygon(MysqlBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Polygon Disjoint Polygon"

    def __init__(self, use_projected_crs=True, subsampling_factor=1):
        super().__init__(create_mysql_adapter(),
                         PolygonDisjointPolygon._title, repeat_count=1)
        self.dataset_suffix = ""
        if use_projected_crs:
            self.dataset_suffix = "_3857"
        self.subsampling_condition = ""
        if subsampling_factor > 1:
            self.subsampling_condition = f"MOD(AS1.OBJECTID, {subsampling_factor}) = 0 AND MOD(AS2.OBJECTID, {subsampling_factor}) = 0 AND"

    def execute(self):
        cmd = f"""SELECT AS1.OBJECTID, AS2.OBJECTID
                FROM {DATABASE_NAME}.airspaces{self.dataset_suffix} AS1, {DATABASE_NAME}.airspaces{self.dataset_suffix} AS2
                WHERE {self.subsampling_condition} st_disjoint(AS1.SHAPE, AS2.SHAPE)
                ;"""
        PolygonDisjointPolygon._logger.info(f"Query: {cmd}")
        return self.adapter.execute(cmd)


class PolygonIntersectsPolygon(MysqlBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Polygon Intersects Polygon"

    def __init__(self, use_projected_crs=True, subsampling_factor=1):
        super().__init__(create_mysql_adapter(),
                         PolygonIntersectsPolygon._title, repeat_count=1)
        self.dataset_suffix = ""
        if use_projected_crs:
            self.dataset_suffix = "_3857"
        self.subsampling_condition = ""
        if subsampling_factor > 1:
            self.subsampling_condition = f"MOD(AS1.OBJECTID, {subsampling_factor}) = 0 AND MOD(AS2.OBJECTID, {subsampling_factor}) = 0 AND"

    def execute(self):
        cmd = f"""SELECT AS1.OBJECTID, AS2.OBJECTID
                FROM {DATABASE_NAME}.airspaces{self.dataset_suffix} AS1, {DATABASE_NAME}.airspaces{self.dataset_suffix} AS2
                WHERE {self.subsampling_condition} st_intersects(AS1.SHAPE, AS2.SHAPE)
                ;"""
        PolygonIntersectsPolygon._logger.info(f"Query: {cmd}")
        return self.adapter.execute(cmd)


class PolygonWithinPolygon(MysqlBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Polygon Within Polygon"

    def __init__(self, use_projected_crs=True, subsampling_factor=1):
        super().__init__(create_mysql_adapter(),
                         PolygonWithinPolygon._title, repeat_count=1)
        self.dataset_suffix = ""
        if use_projected_crs:
            self.dataset_suffix = "_3857"
        self.subsampling_condition = ""
        if subsampling_factor > 1:
            self.subsampling_condition = f"MOD(AS1.OBJECTID, {subsampling_factor}) = 0 AND MOD(AS2.OBJECTID, {subsampling_factor}) = 0 AND"

    def execute(self):
        cmd = f"""SELECT AS1.OBJECTID, AS2.OBJECTID
                FROM {DATABASE_NAME}.airspaces{self.dataset_suffix} AS1, {DATABASE_NAME}.airspaces{self.dataset_suffix} AS2
                WHERE {self.subsampling_condition} st_within(AS1.SHAPE, AS2.SHAPE)
                ;"""
        PolygonWithinPolygon._logger.info(f"Query: {cmd}")
        return self.adapter.execute(cmd)


class RetrievePoints(MysqlBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Retrieve Points"

    def __init__(self, use_projected_crs=True, subsampling_factor=1):
        super().__init__(create_mysql_adapter(),
                         RetrievePoints._title, repeat_count=1)
        self.dataset_suffix = ""
        if use_projected_crs:
            self.dataset_suffix = "_3857"
        self.subsampling_condition = ""
        if subsampling_factor > 1:
            self.subsampling_condition = f"MOD(A.OBJECTID, {subsampling_factor}) = 0 AND"
        self.bounding_box = f"{GEORGIA_BB_3857}, 3857"
        if not use_projected_crs:
            self.bounding_box = f"{GEORGIA_BB_4326}, 4326"

    def execute(self):
        cmd = f"""SELECT A.OBJECTID
                FROM {DATABASE_NAME}.airports{self.dataset_suffix} A
                WHERE {self.subsampling_condition} st_within(A.SHAPE, ST_GeomFromText({self.bounding_box}))
                ;"""
        RetrievePoints._logger.info(f"Query: {cmd}")
        return self.adapter.execute(cmd)


class LongestLine(MysqlBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Longest Line"

    def __init__(self, use_projected_crs=True, subsampling_factor=1):
        super().__init__(create_mysql_adapter(),
                         LongestLine._title, repeat_count=1)
        self.dataset_suffix = ""
        if use_projected_crs:
            self.dataset_suffix = "_3857"
        self.subsampling_condition = ""
        if subsampling_factor > 1:
            self.subsampling_condition = f"WHERE MOD(R.OBJECTID, {subsampling_factor}) = 0"

    def execute(self):
        cmd = f"""SELECT MAX(ST_LENGTH(R.SHAPE))
                FROM {DATABASE_NAME}.routes{self.dataset_suffix} R
                {self.subsampling_condition}
                ;"""
        LongestLine._logger.info(f"Query: {cmd}")
        return self.adapter.execute(cmd)


class TotalLength(MysqlBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Total Length"

    def __init__(self, use_projected_crs=True, subsampling_factor=1):
        super().__init__(create_mysql_adapter(),
                         TotalLength._title, repeat_count=1)
        self.dataset_suffix = ""
        if use_projected_crs:
            self.dataset_suffix = "_3857"
        self.subsampling_condition = ""
        if subsampling_factor > 1:
            self.subsampling_condition = f"WHERE MOD(R.OBJECTID, {subsampling_factor}) = 0"

    def execute(self):
        cmd = f"""SELECT SUM(ST_LENGTH(R.SHAPE))
                FROM {DATABASE_NAME}.routes{self.dataset_suffix} R
                {self.subsampling_condition}
                ;"""
        TotalLength._logger.info(f"Query: {cmd}")
        return self.adapter.execute(cmd)


class RetrieveLines(MysqlBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Total Length"

    def __init__(self, use_projected_crs=True, subsampling_factor=1):
        super().__init__(create_mysql_adapter(),
                         RetrieveLines._title, repeat_count=1)
        self.dataset_suffix = ""
        if use_projected_crs:
            self.dataset_suffix = "_3857"
        self.subsampling_condition = ""
        if subsampling_factor > 1:
            self.subsampling_condition = f"WHERE MOD(R.OBJECTID, {subsampling_factor}) = 0 AND"
        self.bounding_box = f"{GEORGIA_BB_3857}, 3857"
        if not use_projected_crs:
            self.bounding_box = f"{GEORGIA_BB_4326}, 4326"

    def execute(self):
        cmd = f"""SELECT R.OBJECTID
                FROM {DATABASE_NAME}.routes{self.dataset_suffix} R
                WHERE {self.subsampling_condition} st_within(R.SHAPE, ST_GeomFromText({self.bounding_box}))
                ;"""
        RetrieveLines._logger.info(f"Query: {cmd}")
        return self.adapter.execute(cmd)


class LargestArea(MysqlBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Largest Area"

    def __init__(self, use_projected_crs=True, subsampling_factor=1):
        super().__init__(create_mysql_adapter(),
                         LargestArea._title, repeat_count=1)
        self.dataset_suffix = ""
        if use_projected_crs:
            self.dataset_suffix = "_3857"
        self.subsampling_condition = ""
        if subsampling_factor > 1:
            self.subsampling_condition = f"WHERE MOD(AS1.OBJECTID, {subsampling_factor}) = 0"

    def execute(self):
        cmd = f"""SELECT MAX(ST_AREA(AS1.SHAPE))
                FROM {DATABASE_NAME}.airspaces{self.dataset_suffix} AS1
                {self.subsampling_condition}
                ;"""
        LargestArea._logger.info(f"Query: {cmd}")
        return self.adapter.execute(cmd)


class TotalArea(MysqlBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Total Area"

    def __init__(self, use_projected_crs=True, subsampling_factor=1):
        super().__init__(create_mysql_adapter(),
                         TotalArea._title, repeat_count=1)
        self.dataset_suffix = ""
        if use_projected_crs:
            self.dataset_suffix = "_3857"
        self.subsampling_condition = ""
        if subsampling_factor > 1:
            self.subsampling_condition = f"WHERE MOD(AS1.OBJECTID, {subsampling_factor}) = 0"

    def execute(self):
        cmd = f"""SELECT SUM(ST_AREA(AS1.SHAPE))
                FROM {DATABASE_NAME}.airspaces{self.dataset_suffix} AS1
                {self.subsampling_condition}
                ;"""
        TotalArea._logger.info(f"Query: {cmd}")
        return self.adapter.execute(cmd)


class RetrievePolygons(MysqlBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Retrieve Polygons"

    def __init__(self, use_projected_crs=True, subsampling_factor=1):
        super().__init__(create_mysql_adapter(),
                         RetrievePolygons._title, repeat_count=1)
        self.dataset_suffix = ""
        if use_projected_crs:
            self.dataset_suffix = "_3857"
        self.subsampling_condition = ""
        if subsampling_factor > 1:
            self.subsampling_condition = f"MOD(AS1.OBJECTID, {subsampling_factor}) = 0 AND "
        self.bounding_box = f"{GEORGIA_BB_3857}, 3857"
        if not use_projected_crs:
            self.bounding_box = f"{GEORGIA_BB_4326}, 4326"

    def execute(self):
        cmd = f"""SELECT AS1.OBJECTID
                FROM {DATABASE_NAME}.airspaces{self.dataset_suffix} AS1
                WHERE {self.subsampling_condition} st_within(AS1.SHAPE, ST_GeomFromText({self.bounding_box}))
                ;"""
        RetrievePolygons._logger.info(f"Query: {cmd}")
        return self.adapter.execute(cmd)
