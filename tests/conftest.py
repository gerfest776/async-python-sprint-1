import pytest

from api.handlers.tasks import (
    DataAggregationTask,
    DataAnalyzingTask,
    DataCalculationTask,
    DataFetchingTask,
)
from utils import CITIES


@pytest.fixture
def get_fetching_queue():
    return DataFetchingTask().fetch_cities(CITIES)


@pytest.fixture
def calculate_result():
    return DataCalculationTask().calculate_cities(DataFetchingTask().fetch_cities(CITIES))


@pytest.fixture
def make_csv():
    return DataAggregationTask().aggreagate_result(
        DataCalculationTask().calculate_cities(DataFetchingTask().fetch_cities(CITIES))
    )


@pytest.fixture()
def full_pipeline_result():
    return DataAnalyzingTask(
        DataCalculationTask().calculate_cities(DataFetchingTask().fetch_cities(CITIES))
    ).analyze_result()
