import docker
import time
import logging
from gdal.gdaldockerwrapper import GdalDockerWrapper
from mysqlutils.mysqladapter import MySQLAdapter
from postgis_docker_wrapper.postgisadapter import PostgisAdapter
from postgis_docker_wrapper.postgisdockerwrapper import PostgisDockerWrapper


def import_mysql():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    docker_client = docker.from_env()
    gdal_docker_wrapper = GdalDockerWrapper(docker_client)
    mysql_adapter = MySQLAdapter(user="root", password="root-password")

    # Recreate schema
    schema_name = "SpatialDatasets"
    if schema_name in mysql_adapter.get_schemas():
        mysql_adapter.execute("DROP SCHEMA SpatialDatasets")
    mysql_adapter.execute("CREATE SCHEMA SpatialDatasets")

    # Import airports
    logger.info(gdal_docker_wrapper.import_to_mysql(
        "airports_3857/Airports.shp", "airports_3857"))
    logger.info(gdal_docker_wrapper.import_to_mysql(
        "airports/Airports.shp", "airports"))

    # Import airspaces
    logger.info(gdal_docker_wrapper.import_to_mysql(
        "airspace_3857/Class_Airspace.shp", "airspaces_3857"))
    logger.info(gdal_docker_wrapper.import_to_mysql(
        "airspace/Class_Airspace.shp", "airspaces"))

    # Import routes
    logger.info(gdal_docker_wrapper.import_to_mysql(
        "routes_3857/ATS_Route.shp", "routes_3857"))
    logger.info(gdal_docker_wrapper.import_to_mysql(
        "routes/ATS_Route.shp", "routes"))

def import_postgis():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    docker_client = docker.from_env()
    gdal_docker_wrapper = GdalDockerWrapper(docker_client)
    postgis_docker_wrapper = PostgisDockerWrapper(docker_client)
    postgis_docker_wrapper.start_container()
    postgis_adapter = PostgisAdapter(user="postgres", password="root-password", persist = True)

    # Recreate schema
    schema_name = "SpatialDatasets"
    print(postgis_adapter.get_schemas())
    if schema_name in postgis_adapter.get_schemas():
        print("Deleting old schema")
        logger.info(postgis_adapter.execute_nontransaction(f"DROP DATABASE {schema_name}"))

    print("Running createdb")
    # Because ogr2ogr can't do CREATE DATABASE for some reason
    logger.info(postgis_docker_wrapper.inject_command(f"createdb {schema_name} -h 127.0.0.1 -p 5432 -U postgres -w"))
    print("Schema init done")

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

if __name__ == '__main__':
    import_mysql()
    import_postgis()
