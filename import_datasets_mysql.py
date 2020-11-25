import docker
import time
import logging
from gdal.gdaldockerwrapper import GdalDockerWrapper
from mysqlutils.mysqladapter import MySQLAdapter


def main():
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


if __name__ == '__main__':
    main()
