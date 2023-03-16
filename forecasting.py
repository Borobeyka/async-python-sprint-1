from multiprocessing import Pool, cpu_count, Manager
from concurrent.futures import ThreadPoolExecutor

from tasks import (
    DataFetchingTask,
    DataCalculationTask,
    DataAggregationTask,
    DataAnalyzingTask
)
from utils import CITIES

FILENAME = "result.csv"

def forecast_weather():
    """
    Анализ погодных условий по городам
    """
    cities = CITIES.keys()
    queue = Manager().Queue()
    with ThreadPoolExecutor(thread_name_prefix="ThreadFetch") as pool:
        forecasts = list(pool.map(DataFetchingTask.fetch, cities))
    processes = cpu_count() - 1
    calculate = DataCalculationTask(queue)
    aggregate = DataAggregationTask(queue, FILENAME)
    with Pool(processes) as pool:
        pool.map(calculate.run, forecasts)
        queue.put(None)
        pool.apply(aggregate.run)
    analyze = DataAnalyzingTask(FILENAME)
    analyze.run()
    print(f"Наиболее комфортные для проживания города (топ-3): {analyze.comfortables}")

if __name__ == "__main__":  
    forecast_weather()
