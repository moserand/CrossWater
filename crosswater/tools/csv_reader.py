"""Concurrent reading of multiple csv files.
"""

import time


def make_time(time_string):
    """Create seconds since 1970 from date string."""
    time_format = '%Y-%m-%d %H:%M:%S'
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
        self._get_all_ids()

    def _get_all_ids(self):
        """Find all catchment ids and make sure that all files have the same."""
        self.all_ids = [self._get_ids(fobj) for fobj in self.fobjs]
        first_ids = set(self.all_ids[0].keys())
        for ids in self.all_ids[1:]:
            if set(ids.keys()) != first_ids:

                raise ValueError('IDs do not match.')
        self.ids = first_ids

    @staticmethod
    def _get_ids(fobj):
        """Find catchment ids for one file.
        """
        header = next(fobj).split(';')
        return {int(value.strip()[1:-1]): pos for pos, value in
                enumerate(header)}

    @staticmethod
    def _process_line(line, ids):
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
        lines = [next(fobj) for fobj in self.fobjs]
        dates = []
        values = []
        for index, line in enumerate(lines):
            date, value = self._process_line(line, self.all_ids[index])
            dates.append(date)
            values.append(value)
        first_date = dates[0]
        for date in dates[1:]:
            assert first_date == date
        res = {}
        for id_ in self.ids:
            res_id = [date]
            for value in values:
                res_id.append(value[id_])
            res[id_] = res_id
        return res





