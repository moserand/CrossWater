# coding: utf-8

"""Library for reading and writing dbf files.

=========
Copyright
=========
    - 2010 - 2015 hydrocomputing GmbH & Co. KG -- All rights reserved.
    - Author: Dr. Mike Müller
    - Contact: mmueller@hydrocomputing.com

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
    - Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    - Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    - Neither the name of Ad-Mail, Inc nor the
      names of its contributors may be used to endorse or promote products
      derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED ''AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES,
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

from __future__ import print_function

import datetime
import itertools
import struct
import sys

if sys.version_info.major > 3:
    range = xrange
    zip = itertools.izip


MAX_ACTIVE_RECORDS = int(1e6)

class Empty(object):
    """Values for empty record entries.

    It is important to be able to read an write empty fields.
    Unreasonable numbers for the intended application are used.
    Integers are positive and usually not bigger than 1e6.
    Floats are also with few digits.
    Dates usually begin at present time and may go up for several hundred
    years. These numbers have no influences on the parsed results if all fields
    are filled with values.
    Strings and logical values are not effected by this, because empty values
    make sense.
    """
    # We don't want pylint to complain about too few public methods here.
    # pylint: disable-msg=R0903
    empty_integer = -sys.maxsize
    empty_float = -1e300
    empty_date = datetime.date(1, 1, 1)


class DbfReader(object):
    """Reads all records in a Xbase DBF file.

    self.field_names contains the field names.
    self.type contains the type (C = character, N = numeric, F = float,
    D = date, L = logical,m = memo -> ascii).
    self.size contains the total size of the record.
    self.decimal contains the number of decimals.
    self.data contains dict with all columns (!) with field names as keys.
    The deletion flag is read but ignored and records are kept.
    This is intended!

    See DBF format spec at:
        http://www.pgts.com.au/download/public/xbase.htm#DBF_STRUCT
    Inspired by Raymond Hettingers recipe at:
    http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/362715

    """

    def __init__(self, file_name):
        self.file_name = file_name
        self.empty = Empty()
        self.field_names = ['DeletionFlag']
        self.type = ['C']
        self.size = [1]
        self.decimal = [0]

    def read_header(self):
        """Read header information.
        """
        self.fobj = open(self.file_name, 'rb')
        self.numrec, lenheader = struct.unpack('<xxxxLH22x',
                                               self.fobj.read(32))
        numfields = (lenheader - 33) // 32
        for fieldno in range(numfields):
            name, type_, size, decimal = struct.unpack('<11sc4xBB14x',
                                                      self.fobj.read(32))
            name = name.decode('ascii')
            type_ = type_.decode('ascii')
            name = name.split('\0')[0]          # take only first part of name
            self.field_names.append(name)
            self.type.append(type_)
            self.size.append(size)
            self.decimal.append(decimal)
        terminator = self.fobj.read(1)
        assert terminator == b'\r'
        self.format = 's'.join([str(x) for x in self.size]) + 's'
        self.format_size = struct.calcsize(self.format)
        self.header = {'names': self.field_names, 'type': self.type,
                       'size': self.size, 'decimal': self.decimal}

    def read_data(self):
        """Read all data lines.
        """
        names = self.field_names
        active_records = min(self.numrec, MAX_ACTIVE_RECORDS)
        unread_records = max(0, self.numrec - active_records)
        self.data = {}
        format_ = self.format
        read_raw = self.fobj.read
        read_float = self.read_float
        read_integer = self.read_integer
        read_date = self.read_date
        read_logical = self.read_logical
        for name in names:
            self.data[name] = []
        while True:
            records = {}
            for name in names:
                records[name] = [''] * active_records
            for irec in range(active_records):
                rec = struct.unpack(format_, read_raw(self.format_size))
                for iname, name in enumerate(names):
                    records[name][irec] = rec[iname].decode('ascii')
            data = {}
            for m, name in enumerate(names):
                if self.type[m] in 'NF':
                    if self.decimal[m] > 0:
                        try:
                            data[name] = read_float(records[name])
                        except ValueError:
                            print(name, m)
                            raise
                    else:
                        data[name] = read_integer(records[name])
                elif self.type[m] == 'D':
                    data[name] = read_date(records[name])
                elif self.type[m] == 'L':
                    data[name] = read_logical(records[name])
                else:
                    data[name] = records[name]
                self.data[name].extend(data[name])
            if not unread_records:
                break
            active_records = min(unread_records, MAX_ACTIVE_RECORDS)
            unread_records = max(0, unread_records - active_records)

    def read_all(self):
        """Read header and data.
        """
        self.read_header()
        self.read_data()

    def read_integer(self, values):
        """Read integer values.
        """
        def convert(value, empty_integer):
            """Convert string to integer.
            """
            value = value.replace('\0', '').lstrip()
            if value == '':
                return empty_integer
            else:
                try:
                    return int(value)
                except ValueError:
                    return empty_integer
        return [convert(value, self.empty.empty_integer) for value in values]

    def read_float(self, values):
        """Read float value.
        """
        def convert(value, empty_float):
            """Convert string to float.
            """
            value = value.replace('\0', '').lstrip()
            if value == '' or value.strip() == '.':
                return empty_float
            else:
                try:
                    return float(value)
                except ValueError:
                    return empty_float
        return [convert(value, self.empty.empty_float) for value in values]

    def read_date(self, values):
        """Convert date value.
        """
        def convert(value, empty_date):
            """Convert string to date.
            """
            try:
                year, month, day = (int(value[:4]), int(value[4:6]),
                                    int(value[6:8]))
                return datetime.date(year, month, day)
            except ValueError:
                return empty_date
        return [convert(value, self.empty.empty_date) for value in values]

    @staticmethod
    def read_logical(values):
        """Convert logical value.
        """
        def convert(value):
            """Convert string to boolean.
            """
            if value in 'YyTt':
                return True
            elif value in 'NnFf ':
                return False
        return [convert(value) for value in values]

    def close(self):
        """Close the file.
        """
        self.fobj.close()

    def __str__(self):
        """Give a string representation
        """
        return 'DbfReader instance for file %s' % self.file_name


class DbfWriter(object):
    """Writes data in an new dbf file.

        file_name      name of file to write to
        field_names    list with names of fields no longer than 10 characters,
                       no \x00
        types          list with with strings C, M, D, N, F, L (see DbfReader
                        above)
        decimals       list with number of decimal places
        data           dict with fieldname as key and list of values as value
        The Python types of the values are:
            C       string
            M       string
            D       datetime.date object
            N, F
                if deci == 0:
                    integer
                else:
                    float
            L   string ('T', 'F', or '?').
    Inspired by Raymond Hettingers recipe at:
    http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/362715
    """

    def __init__(self, file_name, field_names, type_, size, decimal, data,
                 numrec=None):
        self.file_name = file_name
        self.field_names = field_names
        self.type = type_
        self.size = size
        self.decimal = decimal
        self.data = data
        self.numrec = numrec
        self.empty = Empty()
        # add deletion flag if not given
        if self.field_names[0] != 'DeletionFlag':
            self.field_names.insert(0, 'DeletionFlag')
            self.type.insert(0, 'C')
            self.size.insert(0, 1)
            self.decimal.insert(0, 0)
            self.data['DeletionFlag'] = [' ' for _ in
                                         self.data[self.data.keys()[0]]]

    def write_header(self):
        """Write the header.
        """
        self.fobj = open(self.file_name, 'wb')
        ver = 3
        now = datetime.datetime.now()
        year, month, day = now.year-1900, now.month, now.day
        if self.data:
            numrec = len(self.data[self.field_names[0]])
            if self.numrec:
                assert(numrec == self.numrec)
            self.numrec = numrec
        numfields = len(self.field_names) -1 #deletion flag does not count
        lenheader = numfields * 32 + 33
        lenrecord = sum([s for s in self.size])
        hdr = struct.pack('<BBBBLHH20x', ver, year, month, day, self.numrec,
                          lenheader, lenrecord)
        #hdr = hdr[:-3] + '\xf0' + hdr[-2:] #language driver ID
        self.fobj.write(hdr)

        # field specs
        self.float_formats = []
        for name, typ, size, decimal in zip(self.field_names, self.type,
                                            self.size, self.decimal):
            if name == 'DeletionFlag':
                self.float_formats.append('%%%d.%df' %(size, decimal))
                continue
            name = name.ljust(11)
            name = name.replace(' ', '\x00')
            name = name.encode('ascii')
            typ = typ.encode('ascii')
            fld = struct.pack('<11sc4xBB14x', name, typ, size, decimal)
            self.fobj.write(fld)
            self.float_formats.append('%%%d.%df' %(size, decimal))
        # terminator
        self.fobj.write(b'\r')

    def write_data(self):
        """Write the data.
        """
        names = self.field_names
        active_records = min(self.numrec, MAX_ACTIVE_RECORDS)
        unwritten_records = max(0, self.numrec - active_records)
        # will be overwritten in each loop !
        record = [''] * len(self.field_names)
        start = 0
        end = active_records
        while True:
            converted_data = {}
            for name, typ, size, deci, float_format in zip(
                self.field_names, self.type, self.size, self.decimal,
                self.float_formats):
                if typ in'NF':
                    if deci > 0:
                        converted_data[name] = self.convert_float(
                            self.data[name][start:end], size,
                            float_format, deci)
                    else:
                        converted_data[name] = self.convert_integer(
                            self.data[name][start:end], size)
                elif typ == 'D':
                    converted_data[name] = self.convert_date(
                        self.data[name][start:end])
                elif typ == 'L':
                    converted_data[name] = self.convert_logical(
                        self.data[name][start:end])
                else:
                    converted_data[name] = self.convert_string(
                        self.data[name][start:end], size)
            records = [''] * active_records
            for i in range(active_records):
                for n, name in enumerate(names):
                    converted_entry = converted_data[name][i]
                    if len(converted_entry) != self.size[n]:
                        print('{} expected: {}, got {}'.format(name,
                            self.size[n], len(converted_entry)))
                    record[n] = converted_entry
                records[i] = ''.join(record)
            self.fobj.write(''.join(records).encode('ascii'))
            if not unwritten_records:
                break
            active_records = min(unwritten_records, MAX_ACTIVE_RECORDS)
            unwritten_records = max(0, unwritten_records - active_records)
            start += active_records
            end += active_records
        # End of file
        self.fobj.write(b'\x1A')

    def convert_integer(self, values, size):
        """Convert value to integer.
        """
        def check(value, size, empty_integer):
            """Format integer.
            """
            if value == empty_integer:
                return ' ' * size
            else:
                return str(value).rjust(size)
        return [check(value, size, self.empty.empty_integer) for value in
                values]

    def convert_float(self, values, size, float_format, deci):
        """Convert float to formatted string.
        """

        def conv(value, size, empty_float):
            """Format float.
            """
            if value == empty_float:
                return ' ' * size

            pre_point_lenght = len(str(int(value)))
            adjusted_deci = min(deci, size - pre_point_lenght - 1)
            float_format = '%%%d.%df' % (size, adjusted_deci)
            return float_format % value

        return [conv(value, size, self.empty.empty_float) for value in values]

    def convert_date(self, values):
        """Convert value to date.
        """
        def check(value, empty_date):
            """Format date.
            """
            if value == empty_date:
                return '        '
            else:
                return value.strftime('%Y%m%d')
        return [check(value, self.empty.empty_date) for value in values]

    @staticmethod
    def convert_logical(values):
        """Convert value to logical.
        """
        new_values = ['F'] * len(values)
        for n, value in enumerate(values):
            if value:
                new_values[n] = 'T'
        return new_values

    @staticmethod
    def convert_string(values, size):
        """Convert value to string.
        """
        return  [str(value)[:size].ljust(size) for value in values]

    def write_all(self):
        """Write header and data.
        """
        self.write_header()
        self.write_data()

    def close(self):
        """Close the file.
        """
        self.fobj.close()


class DbfWriterFromReader(DbfWriter):
    '''Convenience class that takes an instance of DbfReader.

       This class is convenient if an existing dbf file is read with DbfReader,
       modified and then written again.
       Instead of specifying `file_name`, `field_mames`, `type`, `size`,
       `decimal`, `data` separately those information will be gathered from
       the DbfReader instance.
    '''

    def __init__(self, file_name, reader):
        assert isinstance(reader, DbfReader)
        DbfWriter.__init__(self, file_name, reader.field_names, reader.type,
                           reader.size, reader.decimal, reader.data)
