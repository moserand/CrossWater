import time

from crosswater.tools.time_helper import show_used_time

def test_show_used_time_1(capfd):

    @show_used_time
    def func():
        time.sleep(1)

    func()

    out, err = capfd.readouterr()
    assert out == 'run time: 1.0 s\n'


def test_show_used_time_2(capfd):

    @show_used_time
    def func():
        time.sleep(0.56)

    func()

    out, err = capfd.readouterr()
    assert out == 'run time: 0.56 s\n'

