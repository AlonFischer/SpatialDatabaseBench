import docker
import time
from gdal.gdaldockerwrapper import GdalDockerWrapper
from mysqlutils.mysqladapter import MySQLAdapter


def main():
    docker_client = docker.from_env()
    gdal_docker_wrapper = GdalDockerWrapper(docker_client)
    mysql_adapter = MySQLAdapter(user="root", password="root-password")
    
    # Recreate schema
    schema_name = "SpatialDatasets"
    if schema_name in mysql_adapter.get_schemas():
        mysql_adapter.execute("DROP SCHEMA SpatialDatasets")
    mysql_adapter.execute("CREATE SCHEMA SpatialDatasets")

    # Import airports
    print(gdal_docker_wrapper.import_to_mysql("airports_projected/Airports.shp", "airports"))
    # Import airspaces
    print(gdal_docker_wrapper.import_to_mysql("airspace_projected/Class_Airspace.shp", "airspaces"))
    # Import routes
    print(gdal_docker_wrapper.import_to_mysql("routes_projected/ATS_Route.shp", "routes"))


if __name__ == '__main__':
    main()
