"""Concurrent reading of multiple csv files.
"""

import time


def make_time_hourly(time_string):
    """Create seconds since 1970 from date string."""
    time_format = '%Y-%m-%d %H:%M:%S'
    return int(time.mktime(time.strptime(time_string, time_format)))


def make_time_daily(time_string):
    """Create seconds since 1970 from date string."""
    time_format = '%Y-%m-%d'
    return int(time.mktime(time.strptime(time_string, time_format)))


class CsvReader(object):
    # pylint: disable=too-few-public-methods
    """Read multiple CSV file at the same time."""

    def __init__(self, *file_names_or_objs):
        self.fobjs = []
        for name_obj in file_names_or_objs:
            if isinstance(name_obj, str):
                fobj = open(name_obj)
            else:
                fobj = name_obj
            self.fobjs.append(fobj)
        self.time_steps = [self._find_time_steps(fobj) for fobj in self.fobjs]
        self.time_converters = [make_time_daily if step == 24 else
                                make_time_hourly for step in self.time_steps]
        self._get_all_ids()
        self.last_dates = [None] * len(self.fobjs)
        self.last_values = [None] * len(self.fobjs)
        self.read_next = [True for _ in self.fobjs]
        self.counter = 0


    def _get_all_ids(self):
        """Find all catchment ids and make sure that all files have the same."""
        self.all_ids = [self._get_ids(fobj) for fobj in self.fobjs]
        first_ids = set(self.all_ids[0].keys())
        for ids in self.all_ids[1:]:
            if set(ids.keys()) != first_ids:
                # TODO: Add more info about non-matching ids.
                raise ValueError('IDs do not match.')
        self.ids = first_ids

    @staticmethod
    def _get_ids(fobj):
        """Find catchment ids for one file.
        """
        header = next(fobj).split(';')
        return {int(value.strip()[1:-1]): pos for pos, value in
                enumerate(header)}

    def _find_time_steps(self, fobj):
        next(fobj)
        line = next(fobj)
        fobj.seek(0)
        date_string = line.split(';', 1)[0]
        date_string_parts = date_string.split()
        if len(date_string_parts) == 2:
            return 1
        elif len(date_string_parts) == 1:
            return 24
        else:
            raise ValueError('Date format {} not supported,'.format(
                data_string))

    @staticmethod
    def _process_line(line, ids, make_time):
        """Returns the date and a dict with ids and values.
        """
        entries = line.split(';')
        date = make_time(entries[0].strip()[1:-1])
        values = [float(item) for item in entries[1:]]
        ids_values = {id_: values[index] for id_, index in ids.items()}
        return date, ids_values

    def __iter__(self):
        return self

    def __next__(self):
        dates = self.last_dates
        values = self.last_values
        self.read_next = [True if self.counter % step == 0 else False for step
                          in self.time_steps]
        self.counter += 1
        lines = [next(fobj) if self.read_next[index] else None
                 for index, fobj in enumerate(self.fobjs)]
        for index, line in enumerate(lines):
            if not self.read_next[index]:
                continue
            try:
                date, value = self._process_line(line, self.all_ids[index],
                                                 self.time_converters[index])
            except ValueError:
                print('index', index)
                raise
            dates[index] = date
            values[index] = value
        res = {}
        for id_ in self.ids:
            res_id = [date]
            for value in values:
                res_id.append(value[id_])
            res[id_] = res_id
        self.last_dates = dates
        self.last_values = values
        return res





