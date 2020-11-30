import docker
from mysqlutils.mysqladapter import MySQLAdapter
from mysqlutils.mysqldockerwrapper import MySqlDockerWrapper
from postgis_docker_wrapper.postgisadapter import PostgisAdapter
from postgis_docker_wrapper.postgisdockerwrapper import PostgisDockerWrapper
from gdal.gdaldockerwrapper import GdalDockerWrapper

import logging

def init():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    docker_client = docker.from_env()
    mysql_docker = MySqlDockerWrapper(docker_client)
    mysql_docker.start_container()
    
    postgis_docker_wrapper = PostgisDockerWrapper(docker_client)
    postgis_docker_wrapper.start_container()

    # Create schema
    mysql_adapter = MySQLAdapter("root", "root-password")
    schema_name = "SpatialDatasets"
    if schema_name in mysql_adapter.get_schemas():
        mysql_adapter.execute(f"DROP SCHEMA {schema_name}")
    mysql_adapter.execute(f"CREATE SCHEMA {schema_name}")

    gdal_docker_wrapper = GdalDockerWrapper(docker_client)
    gdal_docker_wrapper.import_to_mysql(
        "airspace_3857/Class_Airspace.shp", "airspaces_3857", create_spatial_index=True)
    gdal_docker_wrapper.import_to_mysql(
        "airports_3857/Airports.shp", "airports_3857", create_spatial_index=True)
    gdal_docker_wrapper.import_to_mysql(
        "routes_3857/ATS_Route.shp", "routes_3857", create_spatial_index=True)

    # Postgis
    # Recreate schema
    schema_name = "spatialdatasets"
    #print(postgis_adapter.get_schemas())
    #if schema_name in postgis_adapter.get_schemas():
    #    print("Deleting old schema")
    #    logger.info(postgis_adapter.execute_nontransaction(f"DROP DATABASE {schema_name}"))

    print("Running createdb")
    # Because ogr2ogr can't do CREATE DATABASE for some reason
    logger.info(postgis_docker_wrapper.inject_command(f"createdb {schema_name} -h 127.0.0.1 -p 5432 -U postgres -w"))
    print("Schema init done")

    postgis_adapter = PostgisAdapter(user="postgres", password="root-password", persist = True)

    print("Loading extension")
    logger.info(postgis_adapter.execute_nontransaction("CREATE EXTENSION IF NOT EXISTS postgis;"))
    logger.info(postgis_adapter.execute_nontransaction("CREATE EXTENSION IF NOT EXISTS postgis_raster;"))
    logger.info(postgis_adapter.execute_nontransaction("CREATE EXTENSION IF NOT EXISTS postgis_topology;"))
    logger.info(postgis_adapter.execute_nontransaction("CREATE EXTENSION IF NOT EXISTS postgis_sfcgal;"))

    # Import airports
    logger.info(gdal_docker_wrapper.import_to_postgis(
        "airports_3857/Airports.shp", "airports_3857", schema_name=schema_name))
    logger.info(gdal_docker_wrapper.import_to_postgis(
        "airports/Airports.shp", "airports", schema_name=schema_name))

    # Import airspaces
    logger.info(gdal_docker_wrapper.import_to_postgis(
        "airspace_3857/Class_Airspace.shp", "airspaces_3857", schema_name=schema_name))
    logger.info(gdal_docker_wrapper.import_to_postgis(
        "airspace/Class_Airspace.shp", "airspaces", schema_name=schema_name))

    # Import routes
    logger.info(gdal_docker_wrapper.import_to_postgis(
        "routes_3857/ATS_Route.shp", "routes_3857", schema_name=schema_name))
    logger.info(gdal_docker_wrapper.import_to_postgis(
        "routes/ATS_Route.shp", "routes", schema_name=schema_name))
    logger.info(postgis_adapter.execute(
            f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;"))


def start_container():
    docker_client = docker.from_env()
    mysql_docker = MySqlDockerWrapper(docker_client)
    mysql_docker.start_container()
    postgis_docker_wrapper = PostgisDockerWrapper(docker_client)
    postgis_docker_wrapper.start_container()

def cleanup():
    docker_client = docker.from_env()
    mysql_docker = MySqlDockerWrapper(docker_client)

    mysql_docker.stop_container()
    mysql_docker.remove_container()
    mysql_docker.remove_volume()