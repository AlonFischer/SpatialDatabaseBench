import docker
from postgisdockerwrapper import PostgisDockerWrapper
from postgisadapter import PostgisAdapter


def main():
    docker_client = docker.from_env()
    postgis_docker_wrapper = PostgisDockerWrapper(docker_client)
    postgis_docker_wrapper.start_container()
    print(str(postgis_docker_wrapper.get_logs()))
    #postgis_docker_wrapper.stop_container()
    #postgis_docker_wrapper.remove_container()

    postgis_client = PostgisAdapter(user="postgres", password="root-password")
    print(postgis_client.execute("CREATE SCHEMA SpatialDatasets"))
    print(postgis_client.execute("SHOW data_directory"))
    print(postgis_client.execute("SELECT version();"))
    print(postgis_client.execute("SELECT PostGIS_Version();"))


if __name__ == '__main__':
    main()
