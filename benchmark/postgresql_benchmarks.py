#!/usr/bin/env python3
"""
PostgreSQL Benchmark tests
"""
__author__ = "Stephane Carrez"
__copyright__ = "Copyright (C) 2018 Stephane Carrez"
__license__ = 'Apache License, Version 2.0'

import psycopg2
import docker
from benchmark.postgresql_benchmark import PostgreSQLBenchmark
from postgis_docker_wrapper.postgisadapter import PostgisAdapter
from gdal.gdaldockerwrapper import GdalDockerWrapper
import logging
from util.coordinate_transform import transform_4326_to_3857
from util.create_geometry import create_polygon, create_point, create_linestring
from util.misc import convert_decimals_to_ints_in_tuples, convert_none_to_null_in_tuples, tuple_to_str

DATABASE_NAME = "spatialdatasets"
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


class PGLoaderBenchmark(PostgreSQLBenchmark):
    _logger = logging.getLogger(__name__)
    _title = None
    _table_name = None

    def __init__(self, with_index=True):
        super().__init__(self._title, 1)
        docker_client = docker.from_env()
        self.gdal_docker_wrapper = GdalDockerWrapper(docker_client)
        self.with_index = with_index

    def execute(self):
        raise NotImplementedError

    def cleanup(self):
        #print("Cleanup called")
        #self._logger.info(self.adapter_p.execute(
        #    f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;"))

        self.adapter_p.execute(
            f"DROP TABLE {self._table_name}")

class LoadAirspaces(PGLoaderBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Load Airspaces"
    _table_name = "airspaces_temp"

    def execute(self):
        self.gdal_docker_wrapper.import_to_postgis(
            "airspace/Class_Airspace.shp", self._table_name)
        print("Load done")

class LoadAirports(PGLoaderBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Load Airports"
    _table_name = "airports_temp"

    def execute(self):
        self.gdal_docker_wrapper.import_to_postgis(
            "airports/Airports.shp", self._table_name)
        print("Load done")

class LoadRoutes(PGLoaderBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Load Routes"
    _table_name = "routes_temp"

    def execute(self):
        self.gdal_docker_wrapper.import_to_postgis(
            "routes/ATS_Route.shp", self._table_name)
        print("Load done")


class PgSubsampledBenchmark(PostgreSQLBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Base class"
    dataset_suffix = ""
    subsampling_condition = ""
    _object_names = []

    def __init__(self, use_projected_crs=True, subsampling_factor=1):
        super().__init__(self._title, repeat_count=1)
        self.dataset_suffix = ""
        if use_projected_crs:
            self.dataset_suffix = "_3857"
        self.subsampling_condition = ""
        if subsampling_factor > 1:
            for name in self._object_names:
                self.subsampling_condition += f"MOD({name}.OBJECTID, {subsampling_factor}) = 0 AND "

    def execute(self):
        raise NotImplementedError

class PointEqualsPoint(PgSubsampledBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Point Equals Point"
    _object_names = ["A1", "A2"]

    def execute(self):
        cmd = f"""SELECT A1.OBJECTID, A2.OBJECTID
                FROM airports{self.dataset_suffix} A1, airports{self.dataset_suffix} A2
                WHERE {self.subsampling_condition} st_equals(A1.wkb_geometry, A2.wkb_geometry)
                ;"""
        self._logger.info(f"Query: {cmd}")
        return self.adapter_np.execute(cmd)

class PointIntersectsLine(PgSubsampledBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Point Intersects Line"
    _object_names = ["A1", "R2"]

    def execute(self):
        cmd = f"""SELECT A1.OBJECTID, R2.OBJECTID
                FROM airports{self.dataset_suffix} A1, routes{self.dataset_suffix} R2
                WHERE {self.subsampling_condition} st_intersects(A1.wkb_geometry, R2.wkb_geometry)
                ;"""
        self._logger.info(f"Query: {cmd}")
        return self.adapter_np.execute(cmd)

class PointWithinPolygon(PgSubsampledBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Point Within Polygon"
    _object_names = ["A1", "AS2"]

    def execute(self):
        cmd = f"""SELECT A1.OBJECTID, AS2.OBJECTID
                FROM airports{self.dataset_suffix} A1, airspaces{self.dataset_suffix} AS2
                WHERE {self.subsampling_condition} st_within(A1.wkb_geometry, AS2.wkb_geometry)
                ;"""
        self._logger.info(f"Query: {cmd}")
        return self.adapter_np.execute(cmd)

class LineIntersectsPolygon(PgSubsampledBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Line Intersects Polygon"
    _object_names = ["R1", "AS2"]

    def execute(self):
        cmd = f"""SELECT R1.OBJECTID, AS2.OBJECTID
                FROM routes{self.dataset_suffix} R1, airspaces{self.dataset_suffix} AS2
                WHERE {self.subsampling_condition} st_intersects(R1.wkb_geometry, AS2.wkb_geometry)
                ;"""
        self._logger.info(f"Query: {cmd}")
        return self.adapter_np.execute(cmd)

class LineWithinPolygon(PgSubsampledBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Line Within Polygon"
    _object_names = ["A1", "A2"]

    def execute(self):
        cmd = f"""SELECT R1.OBJECTID, AS2.OBJECTID
                FROM routes{self.dataset_suffix} R1, airspaces{self.dataset_suffix} AS2
                WHERE {self.subsampling_condition} st_within(R1.wkb_geometry, AS2.wkb_geometry)
                ;"""
        self._logger.info(f"Query: {cmd}")
        return self.adapter_np.execute(cmd)

class LineIntersectsLine(PgSubsampledBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Line Intersects Line"
    _object_names = ["R1", "R2"]

    def execute(self):
        cmd = f"""SELECT R1.OBJECTID, R2.OBJECTID
                FROM routes{self.dataset_suffix} R1, routes{self.dataset_suffix} R2
                WHERE {self.subsampling_condition} st_intersects(R1.wkb_geometry, R2.wkb_geometry)
                ;"""
        self._logger.info(f"Query: {cmd}")
        return self.adapter_np.execute(cmd)

class PolygonEqualsPolygon(PgSubsampledBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Polygon Equals Polygon"
    _object_names = ["AS1", "AS2"]

    def execute(self):
        cmd = f"""SELECT AS1.OBJECTID, AS2.OBJECTID
                FROM airspaces{self.dataset_suffix} AS1, airspaces{self.dataset_suffix} AS2
                WHERE {self.subsampling_condition} st_equals(AS1.wkb_geometry, AS2.wkb_geometry)
                ;"""
        self._logger.info(f"Query: {cmd}")
        return self.adapter_np.execute(cmd)

class PolygonDisjointPolygon(PgSubsampledBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Polygon Disjoint Polygon"
    _object_names = ["AS1", "AS2"]

    def execute(self):
        cmd = f"""SELECT AS1.OBJECTID, AS2.OBJECTID
                FROM airspaces{self.dataset_suffix} AS1, airspaces{self.dataset_suffix} AS2
                WHERE {self.subsampling_condition} st_disjoint(AS1.wkb_geometry, AS2.wkb_geometry)
                ;"""
        self._logger.info(f"Query: {cmd}")
        return self.adapter_np.execute(cmd)

class PolygonIntersectsPolygon(PgSubsampledBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Polygon Intersects Polygon"
    _object_names = ["AS1", "AS2"]

    def execute(self):
        cmd = f"""SELECT AS1.OBJECTID, AS2.OBJECTID
                FROM airspaces{self.dataset_suffix} AS1, airspaces{self.dataset_suffix} AS2
                WHERE {self.subsampling_condition} st_intersects(AS1.wkb_geometry, AS2.wkb_geometry)
                ;"""
        self._logger.info(f"Query: {cmd}")
        return self.adapter_np.execute(cmd)

class PolygonWithinPolygon(PgSubsampledBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Polygon Within Polygon"
    _object_names = ["AS1", "AS2"]

    def execute(self):
        cmd = f"""SELECT AS1.OBJECTID, AS2.OBJECTID
                FROM airspaces{self.dataset_suffix} AS1, airspaces{self.dataset_suffix} AS2
                WHERE {self.subsampling_condition} st_within(AS1.wkb_geometry, AS2.wkb_geometry)
                ;"""
        self._logger.info(f"Query: {cmd}")
        return self.adapter_np.execute(cmd)

class LongestLine(PgSubsampledBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Longest Line"
    _object_names = ["R"]

    def execute(self):
        cmd = f"""SELECT MAX(ST_LENGTH(R.wkb_geometry))
                FROM routes{self.dataset_suffix} R
                {self.subsampling_condition}
                ;"""
        LongestLine._logger.info(f"Query: {cmd}")
        return self.adapter_np.execute(cmd)

class TotalLength(PgSubsampledBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Total Length"
    _object_names = ["R"]

    def execute(self):
        cmd = f"""SELECT SUM(ST_LENGTH(R.wkb_geometry))
                FROM routes{self.dataset_suffix} R
                {self.subsampling_condition}
                ;"""
        TotalLength._logger.info(f"Query: {cmd}")
        return self.adapter_np.execute(cmd)

class LargestArea(PgSubsampledBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Largest Area"
    _object_names = ["AS1"]

    def execute(self):
        cmd = f"""SELECT MAX(ST_AREA(AS1.wkb_geometry))
                FROM airspaces{self.dataset_suffix} AS1
                {self.subsampling_condition}
                ;"""
        LargestArea._logger.info(f"Query: {cmd}")
        return self.adapter_np.execute(cmd)

class TotalArea(PgSubsampledBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Total Area"
    _object_names = ["AS1"]

    def execute(self):
        cmd = f"""SELECT SUM(ST_AREA(AS1.wkb_geometry))
                FROM airspaces{self.dataset_suffix} AS1
                {self.subsampling_condition}
                ;"""
        TotalArea._logger.info(f"Query: {cmd}")
        return self.adapter_np.execute(cmd)


class PgBoxedBenchmark(PostgreSQLBenchmark):
    _logger = logging.getLogger(__name__)
    _title = None
    _object_names = []

    def __init__(self, use_projected_crs=True, subsampling_factor=1):
        super().__init__(self._title, repeat_count=1)
        self.dataset_suffix = ""
        if use_projected_crs:
            self.dataset_suffix = "_3857"
        self.subsampling_condition = ""
        if subsampling_factor > 1:
            for name in self._object_names:
                self.subsampling_condition += f"MOD({name}.OBJECTID, {subsampling_factor}) = 0 AND "
        self.bounding_box = f"{GEORGIA_BB_3857}, 3857"
        if not use_projected_crs:
            self.bounding_box = f"{GEORGIA_BB_4326}, 4326"
        self.location = f"{ATLANTA_LOC_3857}, 3857"
        if not use_projected_crs:
            self.location = f"{ATLANTA_LOC_4326}, 4326"
        self.line = f"{ROUTE_3857}, 3857"
        if not use_projected_crs:
            self.line = f"{ROUTE_4326}, 4326"

    def execute(self):
        raise NotImplementedError

class RetrievePoints(PgBoxedBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Retrieve Points"
    _object_names = ["A"]

    def execute(self):
        cmd = f"""SELECT A.OBJECTID
                FROM airports{self.dataset_suffix} A
                WHERE {self.subsampling_condition} st_within(A.wkb_geometry, ST_GeomFromText({self.bounding_box}))
                ;"""
        RetrievePoints._logger.info(f"Query: {cmd}")
        return self.adapter_np.execute(cmd)


class RetrieveLines(PgBoxedBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Total Length"
    _object_names = ["R"]

    def execute(self):
        cmd = f"""SELECT R.OBJECTID
                FROM routes{self.dataset_suffix} R
                WHERE {self.subsampling_condition} st_within(R.wkb_geometry, ST_GeomFromText({self.bounding_box}))
                ;"""
        RetrieveLines._logger.info(f"Query: {cmd}")
        return self.adapter_np.execute(cmd)


class RetrievePolygons(PgBoxedBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Retrieve Polygons"
    _object_names = ["AS1"]

    def execute(self):
        cmd = f"""SELECT AS1.OBJECTID
                FROM airspaces{self.dataset_suffix} AS1
                WHERE {self.subsampling_condition} st_within(AS1.wkb_geometry, ST_GeomFromText({self.bounding_box}))
                ;"""
        RetrievePolygons._logger.info(f"Query: {cmd}")
        return self.adapter_np.execute(cmd)


class PointNearPoint(PgBoxedBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Point Near Point"
    _object_names = ["A"]

    def execute(self):
        cmd = f"""SELECT A.OBJECTID
                FROM airports{self.dataset_suffix} A
                WHERE {self.subsampling_condition} st_distance(A.wkb_geometry, ST_GeomFromText({self.location})) < 50000
                ;"""
        PointNearPoint._logger.info(f"Query: {cmd}")
        return self.adapter_np.execute(cmd)


class PointNearPoint2(PgBoxedBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Point Near Point 2"
    _object_names = ["A"]

    def execute(self):
        cmd = f"""SELECT A.OBJECTID, st_distance(A.wkb_geometry, ST_GeomFromText({self.location})) AS dist
                FROM airports{self.dataset_suffix} A
                {self.subsampling_condition}
                ORDER BY dist
                LIMIT 1
                ;"""
        PointNearPoint2._logger.info(f"Query: {cmd}")
        return self.adapter_np.execute(cmd)


class PointNearLine(PgBoxedBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Point Near Line"
    _object_names = ["R"]

    def execute(self):
        cmd = f"""SELECT R.OBJECTID
                FROM routes{self.dataset_suffix} R
                WHERE {self.subsampling_condition} st_distance(R.wkb_geometry, ST_GeomFromText({self.location})) < 500000
                ;"""
        PointNearLine._logger.info(f"Query: {cmd}")
        return self.adapter_np.execute(cmd)


class PointNearLine2(PgBoxedBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Point Near Line 2"
    _object_names = ["R"]

    def execute(self):
        cmd = f"""SELECT R.OBJECTID, st_distance(R.wkb_geometry, ST_GeomFromText({self.location})) AS dist
                FROM routes{self.dataset_suffix} R
                {self.subsampling_condition}
                ORDER BY dist
                LIMIT 1
                ;"""
        PointNearLine2._logger.info(f"Query: {cmd}")
        return self.adapter_np.execute(cmd)


class PointNearPolygon(PgBoxedBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Point Near Polygon"
    _object_names = ["AS1"]

    def execute(self):
        cmd = f"""SELECT AS1.OBJECTID
                FROM airspaces{self.dataset_suffix} AS1
                WHERE {self.subsampling_condition} st_distance(AS1.wkb_geometry, ST_GeomFromText({self.location})) < 500000
                ;"""
        PointNearPolygon._logger.info(f"Query: {cmd}")
        return self.adapter_np.execute(cmd)


class SinglePointWithinPolygon(PgBoxedBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Single Point Within Polygon"
    _object_names = ["AS1"]

    def execute(self):
        cmd = f"""SELECT AS1.OBJECTID
                FROM airspaces{self.dataset_suffix} AS1
                WHERE {self.subsampling_condition} st_contains(AS1.wkb_geometry, ST_GeomFromText({self.location}))
                ;"""
        SinglePointWithinPolygon._logger.info(f"Query: {cmd}")
        return self.adapter_np.execute(cmd)


class LineNearPolygon(PgBoxedBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Line Near Polygon"
    _object_names = ["AS1"]

    def execute(self):
        cmd = f"""SELECT AS1.OBJECTID
                FROM airspaces{self.dataset_suffix} AS1
                WHERE {self.subsampling_condition} st_distance(AS1.wkb_geometry, ST_GeomFromText({self.line})) < 500000
                ;"""
        LineNearPolygon._logger.info(f"Query: {cmd}")
        return self.adapter_np.execute(cmd)


class SingleLineIntersectsPolygon(PgBoxedBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Line Intersects Polygon"
    _object_names = ["AS1"]

    def execute(self):
        cmd = f"""SELECT AS1.OBJECTID
                FROM airspaces{self.dataset_suffix} AS1
                WHERE {self.subsampling_condition} st_intersects(AS1.wkb_geometry, ST_GeomFromText({self.line}))
                ;"""
        SingleLineIntersectsPolygon._logger.info(f"Query: {cmd}")
        return self.adapter_np.execute(cmd)


class InsertNewPoints(PostgreSQLBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Insert New Points"

    def __init__(self, use_projected_crs=True):
        super().__init__(self._title, repeat_count=1)
        self.dataset_suffix = ""
        srid = 4326
        if use_projected_crs:
            self.dataset_suffix = "_3857"
            srid = 3857

        data_to_insert = self.adapter_np.execute(f"""SELECT ST_AsText(SHAPE), global_id, ident, name, latitude, longitude, elevation, icao_id, type_code, servcity, state, country, operstatus, privateuse, iapexists, dodhiflip, far91, far93, mil_code, airanal, us_high, us_low, ak_high, ak_low, us_area, pacific
                                                FROM airports{self.dataset_suffix} A
                                                WHERE A.OBJECTID <= 1000
                                                ;""")
        data_to_insert = convert_decimals_to_ints_in_tuples(data_to_insert)
        # Remove OBJECTID from returned tuples
        data_to_insert = [f"({50000+idx}, ST_GeomFromText('{t[0]}', {srid}), {tuple_to_str(t[1:])[1:]}"
                          for idx, t in enumerate(data_to_insert)]
        self.new_tuples = ', '.join([t for t in data_to_insert])

    def execute(self):
        cmd = f"""INSERT INTO airports{self.dataset_suffix} (objectid, SHAPE, global_id, ident, name, latitude, longitude, elevation, icao_id, type_code, servcity, state, country, operstatus, privateuse, iapexists, dodhiflip, far91, far93, mil_code, airanal, us_high, us_low, ak_high, ak_low, us_area, pacific)
                VALUES {self.new_tuples}
                ;"""
        # InsertNewPoints._logger.info(f"Query: {cmd}")
        self.adapter_p.execute(cmd)
        self.adapter_p.commit()
        return

    def cleanup(self):
        cmd = f"""DELETE FROM airports{self.dataset_suffix}
                WHERE OBJECTID >= 50000
                ;"""
        InsertNewPoints._logger.info(f"Query: {cmd}")
        self.adapter_p.execute(cmd)
        self.adapter_p.commit()
        return


class InsertNewLines(PostgreSQLBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Insert New Lines"

    def __init__(self, use_projected_crs=True):
        super().__init__(self._title, repeat_count=1)
        self.dataset_suffix = ""
        srid = 4326
        if use_projected_crs:
            self.dataset_suffix = "_3857"
            srid = 3857

        data_to_insert = self.adapter_np.execute(f"""SELECT ST_AsText(SHAPE), global_id, ident, level_, wkhr_code, wkhr_rmk, maa_val, maa_uom, mea_e_val, mea_e_uom, mea_w_val, mea_w_uom, gmea_e_val, gmea_e_uom, gmea_w_val, gmea_w_uom, dmea_val, dmea_uom, moca_val, moca_uom, meagap, truetrk, magtrk, revtruetrk, revmagtrk, length_val, copdist, copnav_id, repatcstar, repatcend, direction, freq_class, status, startpt_id, endpt_id, rtport_id, enrinfo_id, widthright, widthleft, width_uom, mca1_val, mca1_uom, mca1_dir, mca2_val, mca2_uom, mca2_dir, mcapt_id, mcapt_type, tflag_code, remarks, ak_low, ak_high, us_low, us_high, type_code, us_area, pacific, nmagtrk, nrevmagtrk, shape__len
                                                FROM routes{self.dataset_suffix} R
                                                WHERE R.OBJECTID <= 1000
                                                ;""")
        data_to_insert = convert_decimals_to_ints_in_tuples(data_to_insert)
        # Remove OBJECTID from returned tuples
        data_to_insert = [f"({50000+idx}, ST_GeomFromText('{t[0]}', {srid}), {tuple_to_str(t[1:])[1:]}"
                          for idx, t in enumerate(data_to_insert)]
        self.new_tuples = ', '.join([t for t in data_to_insert])

    def execute(self):
        cmd = f"""INSERT INTO routes{self.dataset_suffix} (objectid, SHAPE, global_id, ident, level_, wkhr_code, wkhr_rmk, maa_val, maa_uom, mea_e_val, mea_e_uom, mea_w_val, mea_w_uom, gmea_e_val, gmea_e_uom, gmea_w_val, gmea_w_uom, dmea_val, dmea_uom, moca_val, moca_uom, meagap, truetrk, magtrk, revtruetrk, revmagtrk, length_val, copdist, copnav_id, repatcstar, repatcend, direction, freq_class, status, startpt_id, endpt_id, rtport_id, enrinfo_id, widthright, widthleft, width_uom, mca1_val, mca1_uom, mca1_dir, mca2_val, mca2_uom, mca2_dir, mcapt_id, mcapt_type, tflag_code, remarks, ak_low, ak_high, us_low, us_high, type_code, us_area, pacific, nmagtrk, nrevmagtrk, shape__len)
                VALUES {self.new_tuples}
                ;"""
        # InsertNewLines._logger.info(f"Query: {cmd}")
        self.adapter_p.execute(cmd)
        self.adapter_p.commit()
        return

    def cleanup(self):
        cmd = f"""DELETE FROM routes{self.dataset_suffix}
                WHERE OBJECTID >= 50000
                ;"""
        InsertNewLines._logger.info(f"Query: {cmd}")
        self.adapter_p.execute(cmd)
        self.adapter_p.commit()
        return


class InsertNewPolygons(PostgreSQLBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Insert New Polygons"

    def __init__(self, use_projected_crs=True):
        super().__init__(create_mysql_adapter(),
                         InsertNewPolygons._title, repeat_count=1)
        self.dataset_suffix = ""
        srid = 4326
        if use_projected_crs:
            self.dataset_suffix = "_3857"
            srid = 3857

        data_to_insert = self.adapter_np.execute(f"""SELECT ST_AsText(SHAPE), global_id, ident, icao_id, name, upper_desc, upper_val, upper_uom, upper_code, lower_desc, lower_val, lower_uom, lower_code, type_code, local_type, class, mil_code, comm_name, level_, sector, onshore, exclusion, wkhr_code, wkhr_rmk, dst, gmtoffset, cont_agent, city, state, country, adhp_id, us_high, ak_high, ak_low, us_low, us_area, pacific, shape__are, shape__len
                                                FROM airspaces{self.dataset_suffix} AS1
                                                WHERE AS1.OBJECTID <= 1000
                                                ;""")
        data_to_insert = convert_decimals_to_ints_in_tuples(data_to_insert)
        # Remove OBJECTID from returned tuples
        data_to_insert = [f"({50000+idx}, ST_GeomFromText('{t[0]}', {srid}), {tuple_to_str(t[1:])[1:]}"
                          for idx, t in enumerate(data_to_insert)]
        self.new_tuples = ', '.join([t for t in data_to_insert])

    def execute(self):
        cmd = f"""INSERT INTO airspaces{self.dataset_suffix} (objectid, SHAPE, global_id, ident, icao_id, name, upper_desc, upper_val, upper_uom, upper_code, lower_desc, lower_val, lower_uom, lower_code, type_code, local_type, class, mil_code, comm_name, level_, sector, onshore, exclusion, wkhr_code, wkhr_rmk, dst, gmtoffset, cont_agent, city, state, country, adhp_id, us_high, ak_high, ak_low, us_low, us_area, pacific, shape__are, shape__len)
                VALUES {self.new_tuples}
                ;"""
        # InsertNewPolygons._logger.info(f"Query: {cmd}")
        self.adapter_p.execute(cmd)
        self.adapter_p.commit()
        return

    def cleanup(self):
        cmd = f"""DELETE FROM airspaces{self.dataset_suffix}
                WHERE OBJECTID >= 50000
                ;"""
        InsertNewPolygons._logger.info(f"Query: {cmd}")
        self.adapter_p.execute(cmd)
        self.adapter_p.commit()
        return
