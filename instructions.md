# Spatial Database Benchmark

## Data Preparation and Setup

Our project uses MySQL 8 and Postgres 13 + PostGIS 3.0. These will be automatically downloaded in the setup instructions below. Docker is used to manage the databases so no manual installation is necessary.

Our project uses the following datasets for benchmarking:

  * [Airspaces](https://adds-faa.opendata.arcgis.com/datasets/c6a62360338e408cb1512366ad61559e_0) ([hotlink](https://opendata.arcgis.com/datasets/c6a62360338e408cb1512366ad61559e_0.zip))
  * [Airports](https://adds-faa.opendata.arcgis.com/datasets/e747ab91a11045e8b3f8a3efd093d3b5_0) ([hotlink](https://opendata.arcgis.com/datasets/e747ab91a11045e8b3f8a3efd093d3b5_0.zip))
  * [Routes](https://adds-faa.opendata.arcgis.com/datasets/acf64966af5f48a1a40fdbcb31238ba7_0) ([hotlink](https://opendata.arcgis.com/datasets/acf64966af5f48a1a40fdbcb31238ba7_0.zip))

To run the project, the datasets should be downloaded in Shapefile format (can be done using the hotlink or clicking the download button on the right hand side of page and selecting Shapefile). The datasets will be in ZIP format. The datasets should be extracted and the contents placed in the "datasets" folder as follows:

```bash
└── datasets
    ├── airports
    │   ├── Airports.cpg
    │   ├── Airports.dbf
    │   ├── Airports.prj
    │   ├── Airports.shp
    │   ├── Airports.shx
    │   └── Airports.xml
    ├── airspace
    │   ├── Class_Airspace.cpg
    │   ├── Class_Airspace.dbf
    │   ├── Class_Airspace.prj
    │   ├── Class_Airspace.shp
    │   ├── Class_Airspace.shx
    │   └── Class_Airspace.xml
    └── routes
        ├── ATS_Route.cpg
        ├── ATS_Route.dbf
        ├── ATS_Route.prj
        ├── ATS_Route.shp
        ├── ATS_Route.shx
        └── ATS_Route.xml
```

The datasets will be loaded into the databases automatically by the benchmark; no manual loading is needed. The [ogr2ogr](https://gdal.org/programs/ogr2ogr.html) command of [GDAL](https://gdal.org/index.html) is used to project the datasets into cartesian coordinates and import the datasets into both MySQL and PostGIS. As with the databases, GDAL will be automatically downloaded by the benchmark and managed by Docker, so no manual setup is necessary.

The only configuration change made to the databases was to set the parameter `max_parallel_workers_per_gather=0` for Postgres at times to disable parallel query execution. This is managed by the benchmark, and is only listed here for documentation purposes.

## Application and Code

Programming Languages:

* Python 3.8 or later

A recent version of Ubuntu was used to run the benchmark and is recommended. Other operating systems may also work but are not officially supported.

All third party Python libraries used are listed in `requirements.txt`. These will be installed as part of the setup instructions below.

### Setup

Note: These instructions assume a Ubuntu system is being used.

1. Install docker (see <https://docs.docker.com/engine/install/ubuntu/#install-using-the-repository>)
2. Create a docker group and add your user to it: (you may need to log out and log in again for the changes to take affect)
    ```
    sudo groupadd docker
    sudo usermod -aG docker $USER
    ```
3. Pull the necessary docker images (not strictly necessary but otherwise the first time the benchmark runs it will stall while pulling the images)
    ```
    docker pull mysql:8
    docker pull postgis/postgis:13-3.0
    docker pull osgeo/gdal
    ```
4. Optionally, create a virtualenv to keep the project's dependencies isolated
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
5. Install required packages:
    ```
    pip3 install -r requirements.txt
    ```

6. Create projected versions of the datasets
    ```
    python3 create_projected_datasets.py
    ```

### Running the Benchmarks

To run all the benchmarks, run the `run.sh` script. The individual benchmarks developed are also listed below along with instructions to run individual benchmarks. For a shorter test, we recommend running the Spatial Analysis Benchmark with the command `python3 spatial_join_analysis_benchmark.py analysis --init --cleanup --pg-index GIST`.

After running the benchmarks, the raw measurement data can be found in the `results` folder, and the generated graphs can be found in the `figures` folder.

### Individual Benchmarks

* Data Loading Benchmark: measures the time to load each dataset with and without a spatial index in MySQL and PostGIS
  1. Run `python3 data_loading_benchmark.py --cleanup`. Creates an image figures/data_loading_benchmark.png with the results.
* Spatial Join & Analysis Benchmark: measures the time to perform spatial join or analysis queries in MySQL and PostGIS
  1. Run `python3 spatial_join_analysis_benchmark.py <join/analysis> --init --cleanup --pg-index GIST`. Creates an image figures/<join/analysis>_benchmark.png with the results.
* Data Insertion Benchmark: measures the time to insert new data into the tables representing each dataset in MySQL and PostGIS
  1. Run `python3 data_insertion_benchmark.py --init --cleanup`.
  2. Run `python3 data_insertion_benchmark.py --init --cleanup --mysql-noindex --pg-index NONE`.
  3. Run `python3 plotting/data_insertion_benchmark.py`. Creates an image figures/data_insertion_benchmark.png with the results.
* Storage Size Benchmark: measures the disk space used by each dataset.
  1. Run `python3 storage_size_benchmark.py --init --cleanup`. Create an image figures/storage_size_benchmark.png with the results.
* Index Benchmark: measures the time to perform spatial join or analysis queries in MySQL and PostGIS with different indexing options. Note: Running the spatial join queries without an index or with the BRIN index will take a very long time and is not recommended.
  1. Run `python3 spatial_join_analysis_benchmark.py analysis --init --cleanup --pg-index GIST`
  2. Run `python3 spatial_join_analysis_benchmark.py analysis --init --cleanup --mysql-noindex --pg-index NONE`
  3. Run `python3 spatial_join_analysis_benchmark.py analysis --init --cleanup --db pg --pg-index SPGIST`
  4. Run `python3 spatial_join_analysis_benchmark.py analysis --init --cleanup --db pg --pg-index BRIN`
  5. Run `python3 spatial_join_analysis_benchmark.py join --init --cleanup --pg-index GIST`
  6. Run `python3 spatial_join_analysis_benchmark.py join --init --cleanup --db pg --pg-index SPGIST`
  7. Run `python3 plotting/index_benchmark.py <join/analysis>` to plot the results together. Creates an image figures/<join/analysis>_index_benchmark.png with the results.
* CRS Benchmark: measures the time to perform spatial join or analysis queries in MySQL and PostGIS in projected and geographic coordinate systems. Note: Running this benchmark on the spatial join queries will take a very long time and is not recommended.
  1. Run `python3 spatial_join_analysis_benchmark.py <join/analysis> --init --cleanup --pg-index GIST` as in the spatial join & analysis benchmark.
  2. Run `python3 spatial_join_analysis_benchmark.py <join/analysis> --init --cleanup --no-pcs --pg-index GIST` to run the same benchmark with a geographic coordinate system. Creates an image figures/<join/analysis>_benchmark_gcs.png with the results.
  3. Run `python3 plotting/crs_benchmark.py <join/analysis>` to plot the results together. Creates an image figures/<join/analysis>_crs_benchmark.png with the results.
* Subsampling Benchmark: measures the time to perform a subset of the spatial join or analysis queries on several subsets of the datasets.
  1. Run `python3 subsampling_benchmark.py <join/analysis> --init --cleanup --pg-index GIST`. Creates an image figures/subsampling_<join/analysis>_benchmark.png with the results.
* Parallel Execution Benchmark: measures the time to perform spatial join or analysis queries in MySQL, single-threaded PostGIS, and multi-threaded PostGIS.
  1. Run `python3 spatial_join_analysis_benchmark.py <join/analysis> --init --cleanup --pg-index GIST` as in the spatial join & analysis benchmark.
  2. Run `python3 spatial_join_analysis_benchmark.py <join/analysis> --init --cleanup --db pg --parallel --pg-index GIST` to run the same benchmark with only PostGIS and parallel query execution enabled.
  3. Run `python3 plotting/parallel_execution_benchmark.py <join/analysis>` to plot the results together. Creates an image figures/<join/analysis>_parallel_execution.png with the results.

## Code Documentation and References

The structure of the classes and some of the code in the `benchmark` folder came from <https://github.com/stcarrez/sql-benchmark>. We used it as a starting point but modified it heavily. Most of the code was removed, and most of what remains is simply the class structure and declared interface.

All files in the `gdal`, `mysqlutils`, `plotting`, `postgis_docker_wrapper`, `util`, and root folders were written by us. Additionally, within the `benchmark` folder `benchmark_exception.py` was written entirely by us; `benchmark.py`, `mysql_benchmark.py`, and `postgresql_benchmark.py` were partially rewritten by us, and `mysql_benchmarks.py` and `postgresql_benchmarks.py` were entirely rewritten by us.

## Troubleshooting

```
docker.errors.APIError: 409 Client Error: Conflict ("Conflict. The container name "/gdal" is already in use by container "{some random hash}". You have to remove (or rename) that container to be able to reuse that name.")
```

Do a `docker container ls --all` then `docker rm {container name}` on all the containers that conflicts.