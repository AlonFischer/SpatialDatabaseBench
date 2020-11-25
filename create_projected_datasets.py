import docker
import time
import logging
from gdal.gdaldockerwrapper import GdalDockerWrapper


def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    docker_client = docker.from_env()
    gdal_docker_wrapper = GdalDockerWrapper(docker_client)

    # Project airports to EPSG:3857
    logger.info(gdal_docker_wrapper.project_dataset(
        "airports/Airports.shp", "airports_3857/Airports.shp"))
    # Project airspaces to EPSG:3857
    logger.info(gdal_docker_wrapper.project_dataset(
        "airspace/Class_Airspace.shp", "airspace_3857/Class_Airspace.shp"))
    # Project routes to EPSG:3857
    logger.info(gdal_docker_wrapper.project_dataset(
        "routes/ATS_Route.shp", "routes_3857/ATS_Route.shp"))


if __name__ == '__main__':
    main()
