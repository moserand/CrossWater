from io import StringIO

import os

import pytest

from crosswater.tools.csv_reader import CsvReader, make_time


@pytest.fixture
def csv_file0():
    return StringIO(""""346352";"346350";"346351";"346358";"346359"
"2010-01-01 00:00:00";1.0;2.0;3.0;4.0;5.0
"2010-01-01 01:00:00";1.1;2.1;3.1;4.1;5.1""")


@pytest.fixture
def csv_file1():
    return StringIO(""""346352";"346350";"346351";"346358";"346359"
"2010-01-01 00:00:00";11.0;12.0;13.0;14.0;15.0
"2010-01-01 01:00:00";11.1;12.1;13.1;14.1;15.1""")


@pytest.fixture
def csv_file2():
    """Last two columns ar swapped"""
    return StringIO(""""346352";"346350";"346351";"346359";"346358"
"2010-01-01 00:00:00";21.0;22.0;23.0;25.0;24.0
"2010-01-01 01:00:00";21.1;22.1;23.1;25.1;24.1""")



def test_read_lines(csv_file0, csv_file1, csv_file2):
    reader = CsvReader(csv_file0, csv_file1, csv_file2)

    time1 = make_time('2010-01-01 00:00:00')
    time2 = make_time('2010-01-01 01:00:00')

    catchments = next(reader)
    assert catchments[346352] == [time1, 1.0, 11.0, 21.0]
    assert catchments[346350] == [time1, 2.0, 12.0, 22.0]
    assert catchments[346351] == [time1, 3.0, 13.0, 23.0]
    assert catchments[346358] == [time1, 4.0, 14.0, 24.0]
    assert catchments[346359] == [time1, 5.0, 15.0, 25.0]

    catchments = next(reader)
    assert catchments[346352] == [time2, 1.1, 11.1, 21.1]
    assert catchments[346350] == [time2, 2.1, 12.1, 22.1]
    assert catchments[346351] == [time2, 3.1, 13.1, 23.1]
    assert catchments[346358] == [time2, 4.1, 14.1, 24.1]
    assert catchments[346359] == [time2, 5.1, 15.1, 25.1]
