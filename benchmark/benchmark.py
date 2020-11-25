import time
import logging
from benchmark.benchmark_exception import BenchmarkException


class Benchmark:
    """Abstract benchmark base class"""

    _logger = logging.getLogger(__name__)

    def __init__(self, title, repeat_count):
        self.time_measurements = []
        self.repeat_count = repeat_count
        self.title = title

    def get_repeat_count(self):
        return self.repeat_count

    def execute(self):
        """To be implemented by each benchmark child class"""
        pass

    def cleanup(self):
        """Optional method that can be overriden by children.
        Will be executed after each execute, but not included in timings."""
        pass

    def run(self):
        """Run benchmark and record timings"""
        for i in range(self.repeat_count):
            Benchmark._logger.info(
                f"{self.title}: Starting run {i+1} of {self.repeat_count}")
            start = time.perf_counter()
            try:
                self.execute()
            except Exception:
                Benchmark._logger.exception("Exception: ")
                raise BenchmarkException(
                    f"Error running benchmark {self.title}")

            end = time.perf_counter()
            dt = end - start
            self.time_measurements.append(dt)
            Benchmark._logger.info(
                f"{self.title}: Run {i+1} completed in {dt} seconds")

            Benchmark._logger.info(f"{self.title}: Cleaning up run {i+1}")
            self.cleanup()

    def get_time_measurements(self):
        return self.time_measurements

    def get_average_time(self):
        return sum(self.time_measurements) / len(self.time_measurements)
