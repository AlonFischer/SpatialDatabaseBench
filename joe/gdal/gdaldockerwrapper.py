import os
from pathlib import Path
import shutil
import docker
from docker.types import Mount


class GdalDockerWrapper:
    def __init__(self, docker_client):
        self.docker_client = docker_client
        self.image_name = "osgeo/gdal"
        self.container_name = "gdal"
        self.gdal_data_folder = "/data"
        self.dataset_folder = os.getcwd() + '/datasets'

    def run_command(self, cmd):
        data_mount = Mount(self.gdal_data_folder,
                           self.dataset_folder, type='bind', read_only=False)
        try:
            cmd_output = self.docker_client.containers.run(self.image_name,
                                                           cmd,
                                                           name=self.container_name,
                                                           mounts=[data_mount],
                                                           remove=True,
                                                           network_mode='host')
            return cmd_output.decode("utf-8")
        except docker.errors.ContainerError as e:
            return e.stderr.decode("utf-8")
    
    def project_dataset(self, source, dest):
        """ source and dest should be relative the datasets folder
            projects to ESRI:102009 (North America Lambert Conformal Conic; http://epsg.io/102009)
        """

        # Delete and recreate destination directory
        dataset_dest_dir = Path(f"{self.dataset_folder}/{dest}").parent
        if dataset_dest_dir.exists():
            shutil.rmtree(str(dataset_dest_dir))
        dataset_dest_dir.mkdir(parents=True)
        
        cmd = f"ogr2ogr \
            -f 'ESRI Shapefile' {self.gdal_data_folder}/{dest} \
            {self.gdal_data_folder}/{source} \
            -overwrite \
            -t_srs ESRI:102009"
        print(cmd)
        return self.run_command(cmd)
    
    def import_to_mysql(self, source, table_name, create_spatial_index=True, schema_name="SpatialDatasets", host="127.0.0.1", port=3306, user="root", password="root-password"):
        """ source should be relative to the datasets folder
        """

        cmd = f"ogr2ogr \
            -f MySQL MySQL:{schema_name},host={host},port={port},user={user},password={password} \
            {self.gdal_data_folder}/{source} \
            -nln {table_name} \
            -overwrite"
        if not create_spatial_index:
            cmd += " -lco SPATIAL_INDEX=NO"
        print(cmd)
        return self.run_command(cmd)
