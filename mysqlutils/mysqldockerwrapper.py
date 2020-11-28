import docker
import time
import logging


class MySqlDockerWrapper:
    _logger = logging.getLogger(__name__)

    def __init__(self, docker_client):
        self.docker_client = docker_client
        self.image_name = "mysql"
        self.container_name = "mysql"
        self.mysql_version = "8"
        self.root_password = "root-password"
        self.container = None
        self.volume_name = "mysqldata"
        self.mysql_data_folder = "/var/lib/mysql"
        self.volume = None

        try:
            self.get_container()
        except docker.errors.NotFound:
            pass

    def get_container(self):
        try:
            self.container = self.docker_client.containers.get(
                self.container_name)
            MySqlDockerWrapper._logger.info(
                "Found existing MySQL docker container")
        except docker.errors.NotFound:
            MySqlDockerWrapper._logger.info("Container not found")

    def start_container(self):
        try:
            self.container = self.docker_client.containers.get(
                self.container_name)
            self.container.start()
            MySqlDockerWrapper._logger.info(
                "Found existing MySQL docker container")
        except docker.errors.NotFound:
            MySqlDockerWrapper._logger.info(
                "Creating new MySQL docker container")
            self.container = self.docker_client.containers.run(f"{self.image_name}:{self.mysql_version}",
                                                               detach=True,
                                                               name=self.container_name,
                                                               ports={
                                                                   '3306': 3306},
                                                               environment=[
                                                                   f"MYSQL_ROOT_PASSWORD={self.root_password}"],
                                                               volumes={self.volume_name: {'bind': self.mysql_data_folder, 'mode': 'rw'}})

    def stop_container(self):
        if self.container != None:
            MySqlDockerWrapper._logger.info("Stopping MySQL docker container")
            self.container.stop()

    def remove_container(self):
        if self.container != None:
            MySqlDockerWrapper._logger.info("Removing MySQL docker container")
            self.container.remove()

    def get_logs(self):
        if self.container != None:
            return self.container.logs()

    def remove_volume(self):
        try:
            volume = self.docker_client.volumes.get(self.volume_name)
            MySqlDockerWrapper._logger.info("Removing MySQL volume")
            volume.remove()
        except docker.errors.NotFound:
            pass
