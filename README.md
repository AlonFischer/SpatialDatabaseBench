# Spatial Database Benchmark

## Requirements

* A recent version of Ubuntu.
* Python 3

## Setup

1. Install docker (see <https://docs.docker.com/engine/install/ubuntu/#install-using-the-repository>)
2. Create a docker group and add your user to it:
    ```
    sudo groupadd docker
    sudo usermod -aG docker $USER
    ```
3. Pull the necessary docker images (not strictly necessary but otherwise the first time the benchmark runs it will stall while pulling the images)
    ```
    docker pull mysql:8
    docker pull osgeo/gdal
    ```
3. Optionally, create a virtualenv to keep the project's dependencies isolated
    1. Install pip and venv:
        ```
        sudo apt install python3-pip python3-venv
        ```
    2. Install pipx:
        ```
        python3 -m pip install --user pipx
        python3 -m pipx ensurepath
        ```
    3. Install virtualenv:
        ```
        pipx install virtualenv
        ```
    4. Create a virtualenv:
        ```
        virtualenv venv
        ```
    5. Activate the virtual environment: (Note: You will to repeat this step each time you open a new terminal)
        ```
        source venv/bin/activate
        ```
4. Install required packages:
    ```
    pip3 install -r requirements.txt
    ```

5. Run data initialization scripts
    1. 
    ```
    python3 create_projected_datasets.py
    ```
    2. 
    ```
    python3 import_datasets_mysql.py
    ```

## Benchmarks

* Data Loading Benchmark: measures the time to load each dataset with and without a spatial index in MySQL and PostGIS
  1. Run `python3 data_loading_benchmark.py`. Creates an image figures/data_loading_benchmark.png with the results.
* Spatial Join & Analysis Benchmark: measures the time to perform spatial join or analysis queries in MySQL and PostGIS
  1. Run `python3 spatial_join_analysis_benchmark.py <join/analysis>`. Creates an image figures/<join/analysis>_benchmark.png with the results.
* Data Insertion Benchmark: measures the time to insert new data into the tables representing each dataset in MySQL and PostGIS
  1. Run `python3 data_insertion_benchmark.py`. Creates an image figures/data_insertion_benchmark.png with the results.
* Storage Size Benchmark: Measures the disk space used by each dataset.
  1. Run `python3 storage_size_benchmark.py`. Create an image figures/storage_size_benchmark.png with the results.
* Index Benchmark: measures the time to perform spatial join or analysis queries in MySQL and PostGIS. Note: Running this benchmark on the spatial join queries will take a very long time and is not recommended.
  1. Run `python3 spatial_join_analysis_benchmark.py <join/analysis>` as in the spatial join & analysis benchmark.
  2. Run `python3 spatial_join_analysis_benchmark.py <join/analysis> --no-index` to run the same benchmark without spatial indexes on the datasets. Creates an image figures/<join/analysis>_benchmark_no_index.png with the results.
  3. Run `python3 plotting/index_benchmark.py` to plot the results together. Creates an image figures/index_benchmark.png with the results.
* GCS Benchmark: measures the time to perform spatial join or analysis queries in MySQL and PostGIS. Note: Running this benchmark on the spatial join queries will take a very long time and is not recommended.
  1. Run `python3 spatial_join_analysis_benchmark.py <join/analysis>` as in the spatial join & analysis benchmark.
  2. Run `python3 spatial_join_analysis_benchmark.py <join/analysis> --no-pcs` to run the same benchmark without spatial indexes on the datasets. Creates an image figures/<join/analysis>_benchmark_gcs.png with the results.
  3. Run `python3 plotting/crs_benchmark.py` to plot the results together. Creates an image figures/crs_benchmark.png with the results.
* Subsampling Benchmark: measures the time to perform a subset of the spatial join or analysis queries on several subsets of the datasets.
  1. Run `python3 subsampling_benchmark.py <join/analysis>`. Creates an image figures/subsampling_<join/analysis>_benchmark.png with the results.

## Troubleshooting

```
docker.errors.APIError: 409 Client Error: Conflict ("Conflict. The container name "/gdal" is already in use by container "{some random hash}". You have to remove (or rename) that container to be able to reuse that name.")
```

Do a `docker container ls --all` then `docker rm {container name}` on all the containers that conflicts.