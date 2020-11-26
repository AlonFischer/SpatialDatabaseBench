# Spatial Database Benchmark

TODO

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

* data_loading_benchmark.py - measures the time required to load each dataset with and without a spatial index in MySQL and PostGIS
* spatial_join_analysis_benchmark.py - measures the time required to perform spatial join and analysis queries in MySQL and PostGIS
* data_insertion_benchmark.py - measure the time required to insert new data into the tables representing each dataset in MySQL and PostGIS

## Results

## Troubleshooting

```
docker.errors.APIError: 409 Client Error: Conflict ("Conflict. The container name "/gdal" is already in use by container "{some random hash}". You have to remove (or rename) that container to be able to reuse that name.")
```

Do a `docker container ls --all` then `docker rm {container name}` on all the containers that conflicts.