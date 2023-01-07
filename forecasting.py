from api.handlers.tasks import WeatherCityAnalyzer
from utils import CITIES


def forecast_weather():
    """
    Анализ погодных условий по городам
    """
    good_cities = WeatherCityAnalyzer(CITIES).get_good_cities()
    print(f"Наиболее благоприятные города для проживания: {''.join(map(str, good_cities))}")


if __name__ == "__main__":
    forecast_weather()
