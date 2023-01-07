import csv
from os.path import exists
from queue import Queue

import pytest


class Base:
    @pytest.fixture(autouse=True)
    def _get_queue(self, get_fetching_queue):
        self._queue = get_fetching_queue

    @pytest.fixture(autouse=True)
    def _get_result(self, calculate_result):
        self._result = calculate_result

    @pytest.fixture(autouse=True)
    def _make_csv(self, make_csv):
        self._csv = make_csv

    @pytest.fixture(autouse=True)
    def _get_result_full_pipeline(self, full_pipeline_result):
        self._full_result = full_pipeline_result


class TestFetchingTask(Base):
    def test_fetching_putting(self):
        assert isinstance(self._queue, Queue)
        assert not self._queue.empty()

    def test_fetching_result(self):
        while not self._queue.empty():
            task = self._queue.get().result()
            assert isinstance(task, dict)


class TestCalculateResult(Base):
    def test_calculate_result(self):
        assert isinstance(self._result, list)

    def test_inner_dicts(self):
        for city in self._result:
            assert isinstance(city, dict)

    def test_dict_field(self):
        city = self._result[0]

        assert "name" in city
        assert "dates_info" in city
        assert "rating" in city
        assert "temp_avg" in city
        assert "cond_avg" in city
        assert "rating" in city

        dates_info = city["dates_info"][0]

        assert "date" in dates_info
        assert "overage_temperature" in dates_info
        assert "condition" in dates_info


class TestAggregateResult(Base):
    def test_file_exists(self):
        assert exists("cities.csv")

    def test_headers_fields_csv(self):
        with open("cities.csv") as file:
            csv_reader = csv.reader(file, delimiter=",")
            for header in csv_reader:
                assert "Город/День" in header
                assert "Среднее" in header
                assert "Рейтинг" in header
                assert "" in header
                break

    def test_city_fields_csv(self):
        with open("cities.csv") as file:
            csv_reader = csv.reader(file, delimiter=",")
            for i, field in enumerate(csv_reader):
                if i == 0:
                    continue
                elif i == 1:
                    assert "Температура, среднее" in field
                elif i == 2:
                    assert "Без осадков, среднее" in field
                else:
                    break


class TestAnalyzeResult(Base):
    def test_result_type(self):
        assert isinstance(self._full_result, list)

    def test_warn_city(self):
        assert self._full_result == ["Abu Dhabi"]
