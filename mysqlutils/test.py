import docker
from mysqldockerwrapper import MySqlDockerWrapper
from mysqladapter import MySQLAdapter


def main():
    docker_client = docker.from_env()
    mysql_docker_wrapper = MySqlDockerWrapper(docker_client)
    # mysql_docker_wrapper.start_container()
    # print(str(mysql_docker_wrapper.get_logs()))
    # mysql_docker_wrapper.stop_container()
    # mysql_docker_wrapper.remove_container()

    mysql_client = MySQLAdapter(user="root", password="root-password")
    # print(mysql_client.execute("CREATE SCHEMA SpatialDatasets"))
    print(mysql_client.execute("SHOW SCHEMAS"))


if __name__ == '__main__':
    main()
