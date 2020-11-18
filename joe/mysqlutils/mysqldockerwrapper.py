import docker
import time


class MySqlDockerWrapper:
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

    def start_container(self):
        self.container = self.docker_client.containers.run(f"{self.image_name}:{self.mysql_version}",
                                                           detach=True,
                                                           name=self.container_name,
                                                           ports={
                                                               '3306': 3306},
                                                           environment=[
                                                               f"MYSQL_ROOT_PASSWORD={self.root_password}"],
                                                           volumes={self.volume_name: {'bind': self.mysql_data_folder, 'mode': 'rw'}})
        # wait until MySQL is initialized
        logs = str(self.container.logs())
        while '/usr/sbin/mysqld: ready for connections' not in logs:
            time.sleep(0.5)
            logs = str(self.container.logs())

    def stop_container(self):
        if self.container != None:
            self.container.stop()

    def remove_container(self):
        if self.container != None:
            self.container.remove()

    def get_logs(self):
        if self.container != None:
            return self.container.logs()

    def create_volume(self):
        self.volume = self.docker_client.volumes.create("mysqldata")

    def remove_volume(self):
        if self.volume != None:
            self.volume.remove()
