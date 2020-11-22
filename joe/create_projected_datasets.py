import docker
import time
from gdaldockerwrapper import GdalDockerWrapper


def main():
    docker_client = docker.from_env()
    gdal_docker_wrapper = GdalDockerWrapper(docker_client)
    
    # Project airports
    print(gdal_docker_wrapper.project_dataset("airports/Airports.shp", "airports_projected/Airports.shp"))
    # Project airspaces
    print(gdal_docker_wrapper.project_dataset("airspace/Class_Airspace.shp", "airspace_projected/Class_Airspace.shp"))
    # Project routes
    print(gdal_docker_wrapper.project_dataset("routes/ATS_Route.shp", "routes_projected/ATS_Route.shp"))


if __name__ == '__main__':
    main()
