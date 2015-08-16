
import time

from crosswater.tools.time_helper import (
    show_used_time, seconds_to_time_tuple, format_time_tuple)


def test_show_used_time_1(capfd):

    @show_used_time
    def func():
        time.sleep(1)

    func()

    out, err = capfd.readouterr()
    assert out == '\nrun time: 1.0 s\n'


def test_show_used_time_2(capfd):

    @show_used_time
    def func():
        time.sleep(0.56)

    func()

    out, err = capfd.readouterr()
    assert out == '\nrun time: 0.6 s\n'


def test_seconds_to_tuple():
    assert seconds_to_time_tuple(0) == (0, 0, 0, 0, 1)
    assert seconds_to_time_tuple(0.5) == (0, 0, 0, 0, 4)
    assert seconds_to_time_tuple(1) == (0, 0, 0, 1, 0)
    assert seconds_to_time_tuple(1).seconds == 1
    assert seconds_to_time_tuple(61) == (0, 0, 1, 1, 0)
    assert seconds_to_time_tuple(661) == (0, 0, 11, 1, 0)
    assert seconds_to_time_tuple(7861) == (0, 2, 11, 1, 0)
    assert seconds_to_time_tuple(86400) == (1, 0, 0, 0, 0)
    assert seconds_to_time_tuple(86401) == (1, 0, 0, 1, 0)


def test_format_time_tuple():
    assert format_time_tuple((0, 0, 0, 0, 1)) == ' 0.1 s'
    assert format_time_tuple((0, 0, 0, 5, 0)) == ' 5.0 s'
    assert format_time_tuple((0, 0, 0, 10, 0)) == '10 s'
    assert format_time_tuple((0, 0, 1, 0, 0)) == ' 1:00 min'
    assert format_time_tuple((0, 2, 1, 4, 0)) == ' 2:01:04 hours'
    assert format_time_tuple((5, 2, 1, 4, 0)) == ' 5 days 02:01:04'
    assert format_time_tuple((0, 0, 0, 10, 0), fixed=True) == '00:00:10'
    assert format_time_tuple((0, 0, 1, 0, 0), fixed=True) == '00:01:00'
    assert format_time_tuple((0, 2, 1, 4, 0), fixed=True) == '02:01:04'
    assert format_time_tuple((5, 2, 1, 4, 0), fixed=True) == ' 5 days 02:01:04'
