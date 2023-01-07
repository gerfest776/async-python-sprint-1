import csv
import enum
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from multiprocessing import Manager, Process
from multiprocessing import Queue as mpQueue
from multiprocessing.dummy import Pool
from queue import Queue

from api.api_client import YandexWeatherAPI
from api.handlers.base import QueueMixin


class DataFetchingTask(YandexWeatherAPI, QueueMixin):
    def fetch_cities(self, cities: list[str] | str):
        with ThreadPoolExecutor() as pool:
            for city in cities:
                self.queue = pool.submit(self.get_forecasting, city)
        return self._queue


class DataCalculationTask:
    FINDING_HOURS = (9, 20)
    NORMAL_CONDITIONS = ("clear", "partly-cloudy", "overcast", "cloudy")

    def __init__(self):
        self.processes: list[Process] = []
        self.queue = Manager().Queue()

    @staticmethod
    def __count_common_overage(dates_info: list[dict]) -> tuple:
        temp_list = [
            d["overage_temperature"]
            for d in dates_info
            if isinstance(d["overage_temperature"], float)
        ]
        cond_list = [d["condition"] for d in dates_info if isinstance(d["condition"], int)]
        return (
            round(sum(temp_list) / len(temp_list), 1),
            round(sum(cond_list) / len(cond_list), 1),
        )

    @staticmethod
    def __count_overage_temperature(hours: list[dict]) -> float | str:
        temps = []
        for hour in hours:
            if int(hour["hour"]) in range(*DataCalculationTask.FINDING_HOURS):
                temps.append(hour["temp"])
        try:
            return round(sum(temps) / len(temps), 1)
        except ZeroDivisionError:
            return "Недостаточно данных"

    @staticmethod
    def __count_conditions(hours: list[dict]) -> int | str:
        cond = 0
        for hour in hours:
            if (
                int(hour["hour"]) in range(*DataCalculationTask.FINDING_HOURS)
                and hour["condition"] in DataCalculationTask.NORMAL_CONDITIONS
            ):
                cond += 1
        return "Недостаточно данных" if len(hours) != 24 and cond == 0 else cond

    @staticmethod
    def __sorted_and_get_rating(result: list[dict]) -> list[dict]:
        result = sorted(result, key=lambda k: (-k["temp_avg"], -k["cond_avg"]))
        for i, value in enumerate(result):
            value["rating"] = i + 1
        return result

    @classmethod
    def format_response_info(cls, city_response: dict, queue: mpQueue) -> None:
        city_info = {
            "name": city_response["geo_object"]["locality"]["name"],
            "dates_info": [],
            "rating": None,
        }

        for date in city_response["forecasts"]:
            city_info["dates_info"].append(
                {
                    "date": datetime.strptime(date["date"], "%Y-%m-%d").strftime("%d-%m"),
                    "overage_temperature": cls.__count_overage_temperature(date["hours"]),
                    "condition": cls.__count_conditions(date["hours"]),
                }
            )

        temp_avg, cond_avg = DataCalculationTask.__count_common_overage(city_info["dates_info"])
        city_info["temp_avg"] = temp_avg
        city_info["cond_avg"] = cond_avg
        queue.put(city_info)

    def __run_processes(self) -> None:
        [proc.start() for proc in self.processes]

    def __join_processes(self) -> None:
        [proc.join() for proc in self.processes]

    def calculate_cities(self, queue: Queue) -> list[dict]:
        for _ in range(queue.qsize()):
            self.processes.append(
                Process(
                    target=DataCalculationTask.format_response_info,
                    args=(queue.get().result(), self.queue),
                )
            )
        self.__run_processes()
        self.__join_processes()

        result = []
        while not self.queue.empty():
            result.append(self.queue.get())

        result = DataCalculationTask.__sorted_and_get_rating(result)
        return result


class DataAggregationTask(YandexWeatherAPI):
    class CSVFields(str, enum.Enum):
        CITY = "Город/День"
        AVERAGE = "Среднее"
        RATING = "Рейтинг"
        EMPTY = ""
        TEMP_AVERAGE = "Температура, среднее"
        CONDITION_AVERAGE = "Без осадков, среднее"

    def __init__(self) -> None:
        self.csv_locking = threading.Lock()
        self.writer: None | csv.DictWriter = None

    def __create_base_fields(self, city_result: dict) -> list:
        return [
            self.CSVFields.CITY.value,
            self.CSVFields.EMPTY.value,
            *[i["date"] for i in city_result["dates_info"]],
            self.CSVFields.AVERAGE.value,
            self.CSVFields.RATING.value,
        ]

    def __save_csv(self, city) -> None:
        row_temp = {
            self.CSVFields.CITY.value: city["name"],
            self.CSVFields.EMPTY.value: self.CSVFields.TEMP_AVERAGE.value,
            self.CSVFields.AVERAGE.value: city["temp_avg"],
            self.CSVFields.RATING.value: city["rating"],
        }

        row_cond = {
            self.CSVFields.CITY.value: self.CSVFields.EMPTY.value,
            self.CSVFields.EMPTY.value: self.CSVFields.CONDITION_AVERAGE.value,
            self.CSVFields.AVERAGE.value: city["cond_avg"],
            self.CSVFields.RATING.value: self.CSVFields.EMPTY.value,
        }

        for date in city["dates_info"]:
            row_temp[date["date"]] = date["overage_temperature"]
            row_cond[date["date"]] = date["condition"]
        self.writer.writerows([row_temp, row_cond])

    def aggreagate_result(self, result: list[dict]) -> None:
        with open("cities.csv", "w", newline="") as file:
            self.writer = csv.DictWriter(file, fieldnames=self.__create_base_fields(result[0]))
            self.writer.writeheader()

            with ThreadPoolExecutor() as pool:
                pool.map(self.__save_csv, result)


class DataAnalyzingTask(YandexWeatherAPI):
    max_temp: int = 0
    max_cond: int = 0
    max_city_name: str = ""

    def __init__(self, result: list[dict]) -> None:
        self.result = result
        self.__set_max_temp_cond()

    def __set_max_temp_cond(self) -> None:
        max_city = self.result[0]
        self.__class__.max_temp = max_city["temp_avg"]
        self.__class__.max_cond = max_city["cond_avg"]
        self.__class__.max_city_name = max_city["name"]

    @classmethod
    def checking_max_cities(cls, result: dict) -> str | None:
        if result["temp_avg"] == cls.max_temp and result["cond_avg"] == cls.max_cond:
            return result["name"]

    def analyze_result(self):
        with Pool(processes=4) as pool:
            result = pool.map(DataAnalyzingTask.checking_max_cities, self.result)
        return [city for city in result if city is not None]


class WeatherCityAnalyzer:
    def __init__(self, cities: list[str] | str):
        self.cities = cities

    def get_good_cities(self):
        queue = DataFetchingTask().fetch_cities(self.cities)
        result = DataCalculationTask().calculate_cities(queue)
        DataAggregationTask().aggreagate_result(result)
        return DataAnalyzingTask(result).analyze_result()
