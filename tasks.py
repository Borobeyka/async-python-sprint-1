from multiprocessing import Queue, current_process
from api_client import YandexWeatherAPI as ywAPI
from patterns import *
from loguru import logger
from threading import current_thread
import os
import sys
import pandas as pd

from pprint import pprint

class DataFetchingTask:    
    def fetch(city: str) -> CityForecastPattern:
        thread = current_thread()
        logger.debug(f"Thread {thread.name}[{thread.ident}] request YandexWeatherAPI for {city}")
        city_forecast = CityForecastPattern.parse_obj(ywAPI().get_forecasting(city))
        city_forecast.city = city
        return city_forecast


class DataCalculationTask:

    def __init__(self, queue: Queue):
        self.queue = queue

    def get_daily_avg(self, forecast: DayForecastPattern) -> pd.DataFrame:
        daily_avg = pd.DataFrame(
            dict(forecast),
            columns=["day_temp", "clearly"],
        ).transpose()
        for day in forecast:
            average_temp = None
            clearly_hours = None
            df = pd.DataFrame([hour.to_dict() for hour in day.hours])
            if not df.empty:
                df = df[(df["hour"] >= 9) & (df["hour"] < 19)]
            if len(day.hours) and not df.empty:
                average_temp = round(df["temp"].mean(), 2)
                clearly_hours = df[df["condition"] == "clear"] \
                    .agg(["count"])["condition"]["count"]
            daily_avg.loc["day_temp", day.date] = average_temp
            daily_avg.loc["clearly", day.date] = clearly_hours
        daily_avg.fillna("Н/Д", inplace=True)
        return daily_avg
    
    def get_period_avg(self, daily_avg: pd.DataFrame) -> pd.DataFrame:
        period_avg = pd.DataFrame(
            daily_avg.mean(axis=1, numeric_only=True).round(2),
            columns=["period_avg"],
        )
        return period_avg

    def run(self, city_forecast: CityForecastPattern) -> None:
        proc = current_process()
        logger.debug(f"Process {proc.name}[{proc.pid}] calculate average temp for {city_forecast.city}")
        daily_avg = self.get_daily_avg(city_forecast.forecast)
        period_avg = self.get_period_avg(daily_avg)
        city = pd.DataFrame([city_forecast.city], columns=["city"])

        self.queue.put(TableForecastTable(
            city=city,
            dailly_avg=daily_avg,
            period_avg=period_avg
        ))


class DataAggregationTask:

    def __init__(self, queue: Queue, filename: str):
        self.queue = queue
        self.filename = self.check_file(filename)

    def run(self) -> None:
        while item := self.queue.get():
            self.aggregate(item)

    def aggregate(self, data: TableForecastTable) -> None:
        proc = current_process()
        city = data.city.rename(columns={"city": "Город"})
        daily_avg = data.dailly_avg.rename(
            index={
                "day_temp": "Температура, среднее",
                "clearly": "Без осадков, часов",
            }
        ).reset_index().rename(columns={"index": ""})
        period_avg = data.period_avg.rename(
            columns={
                "period_avg": "Среднее"
            }
        ).set_axis([0, 1])
        result = pd.concat([city, daily_avg, period_avg], axis=1)
        with open(self.filename, "a+", encoding="utf-8") as file:
            result.to_csv(
                file,
                mode="a+",
                na_rep="",
                index=False,
                header=self.is_file_empty(),
                encoding="utf-8"
            )
        logger.debug(f"Process {proc.name}[{proc.pid}] aggregate {city.iloc[0]['Город']} to file")

    def is_file_empty(self) -> bool:
        return os.path.getsize(self.filename) == 0

    def check_file(self, filename: str) -> str:
        if os.path.exists(filename):
            os.remove(filename)
        return filename

class DataAnalyzingTask:
    def __init__(self, filename: str):
        self.filename = self.check_file(filename)

    def check_file(self, filename: str) -> str:
        if os.path.exists(filename):
            return filename
        else:
            logger.error(f"File \"{filename}\" not exists")
            sys.exit()
        
    def get_rating(self) -> pd.DataFrame:
        df = pd.read_csv(self.filename, usecols=["Город", "Среднее"])
        df = pd.concat([
                df[::2]["Город"].reset_index(drop=True).rename("city"),
                df[::2]["Среднее"].reset_index(drop=True).rename("avg_temp"),
                df[1::2]["Среднее"].reset_index(drop=True).rename("avg_clearly"),
            ],
            axis=1
        )
        return df.sort_values(["avg_temp", "avg_clearly"], ascending=False).reset_index(drop=True)

    def update_table(self, rating: pd.DataFrame) -> None:
        df = pd.read_csv(self.filename)
        df["rating"] = ""
        for index, row in rating.iterrows():
            df.loc[df["Город"] == row["city"], "rating"] = index + 1
        df.rename(columns={"rating": "Рейтинг"}, inplace=True)
        df.to_csv(self.filename, na_rep="", index=False, encoding="utf-8")
    
    def _get_comfortables(self, rating: pd.DataFrame) -> list[str]:
        return rating.head(3)["city"].to_list()
    
    def run(self) -> None:
        rating = self.get_rating()
        self.update_table(rating)
        self.comfortables = self._get_comfortables(rating)
