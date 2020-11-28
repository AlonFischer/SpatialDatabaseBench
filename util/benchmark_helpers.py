import docker
from mysqlutils.mysqladapter import MySQLAdapter
from mysqlutils.mysqldockerwrapper import MySqlDockerWrapper
from gdal.gdaldockerwrapper import GdalDockerWrapper

def init():
    docker_client = docker.from_env()
    mysql_docker = MySqlDockerWrapper(docker_client)
    mysql_docker.start_container()

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

def cleanup():
    docker_client = docker.from_env()
    mysql_docker = MySqlDockerWrapper(docker_client)

    mysql_docker.stop_container()
    mysql_docker.remove_container()
    mysql_docker.remove_volume()