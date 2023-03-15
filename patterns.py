from pydantic import BaseModel, Field
import pandas as pd

class HourForecastPattern(BaseModel):
    hour: int
    temp: int
    condition: str

    def to_dict(self):
        return self.__dict__

class DayForecastPattern(BaseModel):
    date: str
    hours: list[HourForecastPattern]

class CityForecastPattern(BaseModel):
    city: str = None
    forecast: list[DayForecastPattern] = Field(alias="forecasts")

class TableForecastTable(BaseModel):
    city: pd.DataFrame
    dailly_avg: pd.DataFrame
    period_avg: pd.DataFrame

    class Config:
        arbitrary_types_allowed = True