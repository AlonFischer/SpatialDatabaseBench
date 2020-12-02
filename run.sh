set -x

python3 data_loading_benchmark.py --cleanup

python3 data_insertion_benchmark.py --init --cleanup
python3 data_insertion_benchmark.py --init --cleanup --mysql-noindex --pg-index NONE
python3 plotting/data_insertion_benchmark.py

python3 spatial_join_analysis_benchmark.py analysis --init --cleanup --pg-index GIST
python3 spatial_join_analysis_benchmark.py analysis --init --cleanup --mysql-noindex --pg-index NONE
python3 spatial_join_analysis_benchmark.py analysis --init --cleanup --db pg --pg-index SPGIST
python3 spatial_join_analysis_benchmark.py analysis --init --cleanup --db pg --pg-index BRIN
python3 plotting/index_benchmark.py analysis

python3 spatial_join_analysis_benchmark.py join --init --cleanup --pg-index GIST
python3 spatial_join_analysis_benchmark.py join --init --cleanup --db pg --pg-index SPGIST
python3 plotting/index_benchmark.py join

python3 storage_size_benchmark.py --init --cleanup

python3 spatial_join_analysis_benchmark.py analysis --init --cleanup --no-pcs --pg-index GIST
python3 plotting/crs_benchmark.py analysis
# python3 spatial_join_analysis_benchmark.py join --init --cleanup --no-pcs --pg-index GIST
# python3 plotting/crs_benchmark.py join

python3 subsampling_benchmark.py join --init --cleanup --pg-index GIST
python3 subsampling_benchmark.py analysis --init --cleanup --pg-index GIST

python3 spatial_join_analysis_benchmark.py analysis --init --cleanup --db postgis --parallel --pg-index GIST
python3 plotting/parallel_execution_benchmark.py analysis
python3 spatial_join_analysis_benchmark.py join --init --cleanup --db postgis --parallel --pg-index GIST
python3 plotting/parallel_execution_benchmark.py join