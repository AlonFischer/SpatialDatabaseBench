"""
Mysql benchmark classes
"""
from mysqlutils.mysqladapter import MySQLAdapter
from gdal.gdaldockerwrapper import GdalDockerWrapper
from benchmark.mysql_benchmark import MysqlBenchmark
from util.coordinate_transform import transform_4326_to_3857
from util.create_geometry import create_polygon, create_point, create_linestring
from util.misc import convert_decimals_to_ints_in_tuples, convert_none_to_null_in_tuples, tuple_to_str
import docker
import logging

DATABASE_NAME = "SpatialDatasets"
GEORGIA_BOUNDING_BOX = [(30.3575, -85.6082), (34.9996, -85.6082),
                        (34.9996, -80.696), (30.3575, -80.696), (30.3575, -85.6082)]
GEORGIA_BB_4326 = create_polygon(GEORGIA_BOUNDING_BOX)
GEORGIA_BB_3857 = create_polygon(
    [transform_4326_to_3857(point) for point in GEORGIA_BOUNDING_BOX])
ATLANTA_COORDS = (33.7483, -84.3911)
ATLANTA_LOC_4326 = create_point(ATLANTA_COORDS)
ATLANTA_LOC_3857 = create_point(transform_4326_to_3857(ATLANTA_COORDS))
SAMPLE_ROUTE = [(33.6290830738968, -84.4350692100728),
                (36.1369671135132, -86.6847761162769)]
ROUTE_4326 = create_linestring(SAMPLE_ROUTE)
ROUTE_3857 = create_linestring(
    [transform_4326_to_3857(point) for point in SAMPLE_ROUTE])


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
                         PointEqualsPoint._title, repeat_count=3)
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
                         PointIntersectsLine._title, repeat_count=3)
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
                         PointWithinPolygon._title, repeat_count=3)
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
                         LineIntersectsPolygon._title, repeat_count=3)
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
                         LineWithinPolygon._title, repeat_count=3)
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
        super().__init__(create_mysql_adapter(), LineIntersectsLine._title, repeat_count=3)
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
        super().__init__(create_mysql_adapter(), PolygonEqualsPolygon._title, repeat_count=3)
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
                         PolygonDisjointPolygon._title, repeat_count=3)
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
                         PolygonIntersectsPolygon._title, repeat_count=3)
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
                         PolygonWithinPolygon._title, repeat_count=3)
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
                         RetrievePoints._title, repeat_count=3)
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
                         LongestLine._title, repeat_count=3)
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
                         TotalLength._title, repeat_count=3)
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
    _title = "Retrieve Lines"

    def __init__(self, use_projected_crs=True, subsampling_factor=1):
        super().__init__(create_mysql_adapter(),
                         RetrieveLines._title, repeat_count=3)
        self.dataset_suffix = ""
        if use_projected_crs:
            self.dataset_suffix = "_3857"
        self.subsampling_condition = ""
        if subsampling_factor > 1:
            self.subsampling_condition = f"MOD(R.OBJECTID, {subsampling_factor}) = 0 AND"
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
                         LargestArea._title, repeat_count=3)
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
                         TotalArea._title, repeat_count=3)
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
                         RetrievePolygons._title, repeat_count=3)
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


class PointNearPoint(MysqlBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Point Near Point"

    def __init__(self, use_projected_crs=True, subsampling_factor=1):
        super().__init__(create_mysql_adapter(),
                         PointNearPoint._title, repeat_count=3)
        self.dataset_suffix = ""
        if use_projected_crs:
            self.dataset_suffix = "_3857"
        self.subsampling_condition = ""
        if subsampling_factor > 1:
            self.subsampling_condition = f"MOD(A.OBJECTID, {subsampling_factor}) = 0 AND "
        self.location = f"{ATLANTA_LOC_3857}, 3857"
        if not use_projected_crs:
            self.location = f"{ATLANTA_LOC_4326}, 4326"

    def execute(self):
        cmd = f"""SELECT A.OBJECTID
                FROM {DATABASE_NAME}.airports{self.dataset_suffix} A
                WHERE {self.subsampling_condition} st_distance(A.SHAPE, ST_GeomFromText({self.location}), 'metre') < 50000
                ;"""
        PointNearPoint._logger.info(f"Query: {cmd}")
        return self.adapter.execute(cmd)


class PointNearPoint2(MysqlBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Point Near Point 2"

    def __init__(self, use_projected_crs=True, subsampling_factor=1):
        super().__init__(create_mysql_adapter(),
                         PointNearPoint2._title, repeat_count=3)
        self.dataset_suffix = ""
        if use_projected_crs:
            self.dataset_suffix = "_3857"
        self.subsampling_condition = ""
        if subsampling_factor > 1:
            self.subsampling_condition = f"WHERE MOD(A.OBJECTID, {subsampling_factor}) = 0"
        self.location = f"{ATLANTA_LOC_3857}, 3857"
        if not use_projected_crs:
            self.location = f"{ATLANTA_LOC_4326}, 4326"

    def execute(self):
        cmd = f"""SELECT A.OBJECTID, st_distance(A.SHAPE, ST_GeomFromText({self.location}), 'metre') AS dist
                FROM {DATABASE_NAME}.airports{self.dataset_suffix} A
                {self.subsampling_condition}
                ORDER BY dist
                LIMIT 1
                ;"""
        PointNearPoint2._logger.info(f"Query: {cmd}")
        return self.adapter.execute(cmd)


class PointNearLine(MysqlBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Point Near Line"

    def __init__(self, use_projected_crs=True, subsampling_factor=1):
        super().__init__(create_mysql_adapter(),
                         PointNearLine._title, repeat_count=3)
        self.dataset_suffix = ""
        if use_projected_crs:
            self.dataset_suffix = "_3857"
        self.subsampling_condition = ""
        if subsampling_factor > 1:
            self.subsampling_condition = f"MOD(R.OBJECTID, {subsampling_factor}) = 0 AND "
        self.location = f"{ATLANTA_LOC_3857}, 3857"
        if not use_projected_crs:
            self.location = f"{ATLANTA_LOC_4326}, 4326"

    def execute(self):
        cmd = f"""SELECT R.OBJECTID
                FROM {DATABASE_NAME}.routes{self.dataset_suffix} R
                WHERE {self.subsampling_condition} st_distance(R.SHAPE, ST_GeomFromText({self.location}), 'metre') < 500000
                ;"""
        PointNearLine._logger.info(f"Query: {cmd}")
        return self.adapter.execute(cmd)


class PointNearLine2(MysqlBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Point Near Line 2"

    def __init__(self, use_projected_crs=True, subsampling_factor=1):
        super().__init__(create_mysql_adapter(),
                         PointNearLine2._title, repeat_count=3)
        self.dataset_suffix = ""
        if use_projected_crs:
            self.dataset_suffix = "_3857"
        self.subsampling_condition = ""
        if subsampling_factor > 1:
            self.subsampling_condition = f"WHERE MOD(R.OBJECTID, {subsampling_factor}) = 0"
        self.location = f"{ATLANTA_LOC_3857}, 3857"
        if not use_projected_crs:
            self.location = f"{ATLANTA_LOC_4326}, 4326"

    def execute(self):
        cmd = f"""SELECT R.OBJECTID, st_distance(R.SHAPE, ST_GeomFromText({self.location}), 'metre') AS dist
                FROM {DATABASE_NAME}.routes{self.dataset_suffix} R
                {self.subsampling_condition}
                ORDER BY dist
                LIMIT 1
                ;"""
        PointNearLine2._logger.info(f"Query: {cmd}")
        return self.adapter.execute(cmd)


class PointNearPolygon(MysqlBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Point Near Polygon"

    def __init__(self, use_projected_crs=True, subsampling_factor=1):
        super().__init__(create_mysql_adapter(),
                         PointNearPolygon._title, repeat_count=3)
        self.dataset_suffix = ""
        if use_projected_crs:
            self.dataset_suffix = "_3857"
        self.subsampling_condition = ""
        if subsampling_factor > 1:
            self.subsampling_condition = f"MOD(AS1.OBJECTID, {subsampling_factor}) = 0 AND "
        self.location = f"{ATLANTA_LOC_3857}, 3857"
        if not use_projected_crs:
            self.location = f"{ATLANTA_LOC_4326}, 4326"

    def execute(self):
        cmd = f"""SELECT AS1.OBJECTID
                FROM {DATABASE_NAME}.airspaces{self.dataset_suffix} AS1
                WHERE {self.subsampling_condition} st_distance(AS1.SHAPE, ST_GeomFromText({self.location}), 'metre') < 500000
                ;"""
        PointNearPolygon._logger.info(f"Query: {cmd}")
        return self.adapter.execute(cmd)


class SinglePointWithinPolygon(MysqlBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Single Point Within Polygon"

    def __init__(self, use_projected_crs=True, subsampling_factor=1):
        super().__init__(create_mysql_adapter(),
                         SinglePointWithinPolygon._title, repeat_count=3)
        self.dataset_suffix = ""
        if use_projected_crs:
            self.dataset_suffix = "_3857"
        self.subsampling_condition = ""
        if subsampling_factor > 1:
            self.subsampling_condition = f"MOD(AS1.OBJECTID, {subsampling_factor}) = 0 AND "
        self.location = f"{ATLANTA_LOC_3857}, 3857"
        if not use_projected_crs:
            self.location = f"{ATLANTA_LOC_4326}, 4326"

    def execute(self):
        cmd = f"""SELECT AS1.OBJECTID
                FROM {DATABASE_NAME}.airspaces{self.dataset_suffix} AS1
                WHERE {self.subsampling_condition} st_contains(AS1.SHAPE, ST_GeomFromText({self.location}))
                ;"""
        SinglePointWithinPolygon._logger.info(f"Query: {cmd}")
        return self.adapter.execute(cmd)


class LineNearPolygon(MysqlBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Line Near Polygon"

    def __init__(self, use_projected_crs=True, subsampling_factor=1):
        super().__init__(create_mysql_adapter(),
                         LineNearPolygon._title, repeat_count=3)
        self.dataset_suffix = ""
        if use_projected_crs:
            self.dataset_suffix = "_3857"
        self.subsampling_condition = ""
        if subsampling_factor > 1:
            self.subsampling_condition = f"MOD(AS1.OBJECTID, {subsampling_factor}) = 0 AND "
        self.line = f"{ROUTE_3857}, 3857"
        if not use_projected_crs:
            self.line = f"{ROUTE_4326}, 4326"

    def execute(self):
        cmd = f"""SELECT AS1.OBJECTID
                FROM {DATABASE_NAME}.airspaces{self.dataset_suffix} AS1
                WHERE {self.subsampling_condition} st_distance(AS1.SHAPE, ST_GeomFromText({self.line}), 'metre') < 500000
                ;"""
        LineNearPolygon._logger.info(f"Query: {cmd}")
        return self.adapter.execute(cmd)


class SingleLineIntersectsPolygon(MysqlBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Line Intersects Polygon"

    def __init__(self, use_projected_crs=True, subsampling_factor=1):
        super().__init__(create_mysql_adapter(),
                         SingleLineIntersectsPolygon._title, repeat_count=3)
        self.dataset_suffix = ""
        if use_projected_crs:
            self.dataset_suffix = "_3857"
        self.subsampling_condition = ""
        if subsampling_factor > 1:
            self.subsampling_condition = f"MOD(AS1.OBJECTID, {subsampling_factor}) = 0 AND "
        self.line = f"{ROUTE_3857}, 3857"
        if not use_projected_crs:
            self.line = f"{ROUTE_4326}, 4326"

    def execute(self):
        cmd = f"""SELECT AS1.OBJECTID
                FROM {DATABASE_NAME}.airspaces{self.dataset_suffix} AS1
                WHERE {self.subsampling_condition} st_intersects(AS1.SHAPE, ST_GeomFromText({self.line}))
                ;"""
        SingleLineIntersectsPolygon._logger.info(f"Query: {cmd}")
        return self.adapter.execute(cmd)


class InsertNewPoints(MysqlBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Insert New Points"

    def __init__(self, use_projected_crs=True):
        super().__init__(create_mysql_adapter(),
                         InsertNewPoints._title, repeat_count=3)
        self.dataset_suffix = ""
        srid = 4326
        if use_projected_crs:
            self.dataset_suffix = "_3857"
            srid = 3857

        data_to_insert = self.adapter.execute(f"""SELECT ST_AsText(SHAPE), global_id, ident, name, latitude, longitude, elevation, icao_id, type_code, servcity, state, country, operstatus, privateuse, iapexists, dodhiflip, far91, far93, mil_code, airanal, us_high, us_low, ak_high, ak_low, us_area, pacific
                                                FROM {DATABASE_NAME}.airports{self.dataset_suffix} A
                                                WHERE A.OBJECTID <= 1000
                                                ;""")
        data_to_insert = convert_decimals_to_ints_in_tuples(data_to_insert)
        # Remove OBJECTID from returned tuples
        data_to_insert = [f"({50000+idx}, ST_GeomFromText('{t[0]}', {srid}), {tuple_to_str(t[1:])[1:]}"
                          for idx, t in enumerate(data_to_insert)]
        self.new_tuples = ', '.join([t for t in data_to_insert])

    def execute(self):
        cmd = f"""INSERT INTO {DATABASE_NAME}.airports{self.dataset_suffix} (objectid, SHAPE, global_id, ident, name, latitude, longitude, elevation, icao_id, type_code, servcity, state, country, operstatus, privateuse, iapexists, dodhiflip, far91, far93, mil_code, airanal, us_high, us_low, ak_high, ak_low, us_area, pacific)
                VALUES {self.new_tuples}
                ;"""
        # InsertNewPoints._logger.info(f"Query: {cmd}")
        self.adapter.execute(cmd)
        self.adapter.commit()
        return

    def cleanup(self):
        cmd = f"""DELETE FROM {DATABASE_NAME}.airports{self.dataset_suffix}
                WHERE OBJECTID >= 50000
                ;"""
        InsertNewPoints._logger.info(f"Query: {cmd}")
        self.adapter.execute(cmd)
        self.adapter.commit()
        return


class InsertNewLines(MysqlBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Insert New Lines"

    def __init__(self, use_projected_crs=True):
        super().__init__(create_mysql_adapter(),
                         InsertNewLines._title, repeat_count=3)
        self.dataset_suffix = ""
        srid = 4326
        if use_projected_crs:
            self.dataset_suffix = "_3857"
            srid = 3857

        data_to_insert = self.adapter.execute(f"""SELECT ST_AsText(SHAPE), global_id, ident, level_, wkhr_code, wkhr_rmk, maa_val, maa_uom, mea_e_val, mea_e_uom, mea_w_val, mea_w_uom, gmea_e_val, gmea_e_uom, gmea_w_val, gmea_w_uom, dmea_val, dmea_uom, moca_val, moca_uom, meagap, truetrk, magtrk, revtruetrk, revmagtrk, length_val, copdist, copnav_id, repatcstar, repatcend, direction, freq_class, status, startpt_id, endpt_id, rtport_id, enrinfo_id, widthright, widthleft, width_uom, mca1_val, mca1_uom, mca1_dir, mca2_val, mca2_uom, mca2_dir, mcapt_id, mcapt_type, tflag_code, remarks, ak_low, ak_high, us_low, us_high, type_code, us_area, pacific, nmagtrk, nrevmagtrk, shape__len
                                                FROM {DATABASE_NAME}.routes{self.dataset_suffix} R
                                                WHERE R.OBJECTID <= 1000
                                                ;""")
        data_to_insert = convert_decimals_to_ints_in_tuples(data_to_insert)
        # Remove OBJECTID from returned tuples
        data_to_insert = [f"({50000+idx}, ST_GeomFromText('{t[0]}', {srid}), {tuple_to_str(t[1:])[1:]}"
                          for idx, t in enumerate(data_to_insert)]
        self.new_tuples = ', '.join([t for t in data_to_insert])

    def execute(self):
        cmd = f"""INSERT INTO {DATABASE_NAME}.routes{self.dataset_suffix} (objectid, SHAPE, global_id, ident, level_, wkhr_code, wkhr_rmk, maa_val, maa_uom, mea_e_val, mea_e_uom, mea_w_val, mea_w_uom, gmea_e_val, gmea_e_uom, gmea_w_val, gmea_w_uom, dmea_val, dmea_uom, moca_val, moca_uom, meagap, truetrk, magtrk, revtruetrk, revmagtrk, length_val, copdist, copnav_id, repatcstar, repatcend, direction, freq_class, status, startpt_id, endpt_id, rtport_id, enrinfo_id, widthright, widthleft, width_uom, mca1_val, mca1_uom, mca1_dir, mca2_val, mca2_uom, mca2_dir, mcapt_id, mcapt_type, tflag_code, remarks, ak_low, ak_high, us_low, us_high, type_code, us_area, pacific, nmagtrk, nrevmagtrk, shape__len)
                VALUES {self.new_tuples}
                ;"""
        # InsertNewLines._logger.info(f"Query: {cmd}")
        self.adapter.execute(cmd)
        self.adapter.commit()
        return

    def cleanup(self):
        cmd = f"""DELETE FROM {DATABASE_NAME}.routes{self.dataset_suffix}
                WHERE OBJECTID >= 50000
                ;"""
        InsertNewLines._logger.info(f"Query: {cmd}")
        self.adapter.execute(cmd)
        self.adapter.commit()
        return


class InsertNewPolygons(MysqlBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Insert New Polygons"

    def __init__(self, use_projected_crs=True):
        super().__init__(create_mysql_adapter(),
                         InsertNewPolygons._title, repeat_count=3)
        self.dataset_suffix = ""
        srid = 4326
        if use_projected_crs:
            self.dataset_suffix = "_3857"
            srid = 3857

        data_to_insert = self.adapter.execute(f"""SELECT ST_AsText(SHAPE), global_id, ident, icao_id, name, upper_desc, upper_val, upper_uom, upper_code, lower_desc, lower_val, lower_uom, lower_code, type_code, local_type, class, mil_code, comm_name, level_, sector, onshore, exclusion, wkhr_code, wkhr_rmk, dst, gmtoffset, cont_agent, city, state, country, adhp_id, us_high, ak_high, ak_low, us_low, us_area, pacific, shape__are, shape__len
                                                FROM {DATABASE_NAME}.airspaces{self.dataset_suffix} AS1
                                                WHERE AS1.OBJECTID <= 1000
                                                ;""")
        data_to_insert = convert_decimals_to_ints_in_tuples(data_to_insert)
        # Remove OBJECTID from returned tuples
        data_to_insert = [f"({50000+idx}, ST_GeomFromText('{t[0]}', {srid}), {tuple_to_str(t[1:])[1:]}"
                          for idx, t in enumerate(data_to_insert)]
        self.new_tuples = ', '.join([t for t in data_to_insert])

    def execute(self):
        cmd = f"""INSERT INTO {DATABASE_NAME}.airspaces{self.dataset_suffix} (objectid, SHAPE, global_id, ident, icao_id, name, upper_desc, upper_val, upper_uom, upper_code, lower_desc, lower_val, lower_uom, lower_code, type_code, local_type, class, mil_code, comm_name, level_, sector, onshore, exclusion, wkhr_code, wkhr_rmk, dst, gmtoffset, cont_agent, city, state, country, adhp_id, us_high, ak_high, ak_low, us_low, us_area, pacific, shape__are, shape__len)
                VALUES {self.new_tuples}
                ;"""
        # InsertNewPolygons._logger.info(f"Query: {cmd}")
        self.adapter.execute(cmd)
        self.adapter.commit()
        return

    def cleanup(self):
        cmd = f"""DELETE FROM {DATABASE_NAME}.airspaces{self.dataset_suffix}
                WHERE OBJECTID >= 50000
                ;"""
        InsertNewPolygons._logger.info(f"Query: {cmd}")
        self.adapter.execute(cmd)
        self.adapter.commit()
        return


class AirspacesSize(MysqlBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Airspaces Size"

    def __init__(self, use_projected_crs=True):
        super().__init__(create_mysql_adapter(),
                         AirspacesSize._title, repeat_count=1)
        self.dataset_suffix = ""
        if use_projected_crs:
            self.dataset_suffix = "_3857"

    def execute(self):
        cmd = f"""SELECT (data_length+index_length)/power(1024,2) tablesize_mb
                FROM information_schema.tables
                WHERE table_schema='{DATABASE_NAME}' and table_name='airspaces{self.dataset_suffix}'
                ;"""
        AirspacesSize._logger.info(cmd)
        return self.adapter.execute(cmd)


class AirportsSize(MysqlBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Airports Size"

    def __init__(self, use_projected_crs=True):
        super().__init__(create_mysql_adapter(),
                         AirportsSize._title, repeat_count=1)
        self.dataset_suffix = ""
        if use_projected_crs:
            self.dataset_suffix = "_3857"

    def execute(self):
        cmd = f"""SELECT (data_length+index_length)/power(1024,2) tablesize_mb
                FROM information_schema.tables
                WHERE table_schema='{DATABASE_NAME}' and table_name='airports{self.dataset_suffix}'
                ;"""
        AirportsSize._logger.info(cmd)
        return self.adapter.execute(cmd)


class RoutesSize(MysqlBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Routes Size"

    def __init__(self, use_projected_crs=True):
        super().__init__(create_mysql_adapter(),
                         RoutesSize._title, repeat_count=1)
        self.dataset_suffix = ""
        if use_projected_crs:
            self.dataset_suffix = "_3857"

    def execute(self):
        cmd = f"""SELECT (data_length+index_length)/power(1024,2) tablesize_mb
                FROM information_schema.tables
                WHERE table_schema='{DATABASE_NAME}' and table_name='routes{self.dataset_suffix}'
                ;"""
        RoutesSize._logger.info(cmd)
        return self.adapter.execute(cmd)
