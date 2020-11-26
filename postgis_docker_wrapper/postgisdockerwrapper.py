import docker
import time
import logging


class PostgisDockerWrapper:
    _logger = logging.getLogger(__name__)

    def __init__(self, docker_client):
        self.docker_client = docker_client
        self.image_name = "postgis/postgis"
        self.container_name = "postgis"
        self.postgis_version = "13-3.0"
        self.root_password = "root-password"
        self.container = None
        self.volume_name = "postgisdata"
        self.postgis_data_folder = "/var/lib/postgresql"
        self.volume = None

    def get_container(self):
        try:
            self.container = self.docker_client.containers.get(
                self.container_name)
            self.container.start()
            PostgisDockerWrapper._logger.info(
                "Found existing Postgis docker container")
        except docker.errors.NotFound:
            print("Container not found")

    def start_container(self):
        try:
            self.container = self.docker_client.containers.get(
                self.container_name)
            self.container.start()
            PostgisDockerWrapper._logger.info(
                "Found existing Postgis docker container")
        except docker.errors.NotFound:
            PostgisDockerWrapper._logger.info(
                "Creating new Postgis docker container")
            self.container = self.docker_client.containers.run(f"{self.image_name}:{self.postgis_version}",
                                                               detach=True,
                                                               name=self.container_name,
                                                               ports={
                                                                   '5432': 5432},
                                                               environment=[
                                                                   f"POSTGRES_PASSWORD={self.root_password}"],
                                                               volumes={self.volume_name: {'bind': self.postgis_data_folder, 'mode': 'rw'}},
                                                               command="postgres")
        # wait until MySQL is initialized
        # TODO: Fix this
        logs = str(self.container.logs())
        while 'listening on IPv6 address "::", port 5432' not in logs:
            #print(logs)
            time.sleep(0.5)
            logs = str(self.container.logs())

    def inject_command(self, cmd):
        if self.container != None:
            print(f"Executing: {cmd}")
            return self.container.exec_run(cmd)

    def stop_container(self):
        if self.container != None:
            PostgisDockerWrapper._logger.info("Stopping Postgis docker container")
            self.container.stop()

    def remove_container(self):
        if self.container != None:
            PostgisDockerWrapper._logger.info("Removing Postgis docker container")
            self.container.remove()

    def get_logs(self):
        if self.container != None:
            return self.container.logs()

    def remove_volume(self):
        try:
            volume = self.docker_client.volumes.get(self.volume_name)
            PostgisDockerWrapper._logger.info("Removing Postgis volume")
            volume.remove()
        except docker.errors.NotFound:
            pass
