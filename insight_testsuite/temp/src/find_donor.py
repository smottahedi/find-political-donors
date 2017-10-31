#!/usr/bin/env python3

"""
    This script extract pip-delimited contributions information from a txt file
    and stores the  information in two file bellow:

        medianvals_by_zip.txt: median contribution, number of contribution and
            total contribution to each combination of recipient and zipcode.

        medianvals_by_date.txt: median contribution, number of contributions
        and total contribution to each combination of recipient and
        contribution date sorted first by recipient id and secondly by date of
        contribution.
"""

import shelve
from math import modf
from numpy import median
from time import time
import argparse
from os.path import isfile
import re
from datetime import datetime


def process_line(line, delimiter="|"):
    """
    Split delimited line and return a tuple containing required items. Checks
    the validity of data.

    Args:
        line (str): line to be processed.
        delimiter(str): delimiter used when spliting.

    Returns:
        tuple:return tuple of five values.

        CMTE_ID: identifies the flier, which for our purposes is the recipient
                 of this contribution
        ZIP_CODE: zip code of the contributor (we only want the first five
                  digits/characters)
        TRANSACTION_DT: date of the transaction
        TRANSACTION_AMT: amount of the transaction
    """
    row = line.split(delimiter)
    if len(row) != 21:
        raise ValueError
    if row[15]:
        raise ValueError
    if len(row[0]) == 0:
        raise ValueError
    if len(row[14]) == 0:
        raise ValueError
    return row[0], row[10][:5], row[13], row[14]


def check_zipcode(zipcode):
    """check the validity of zipcode.

    Args:
        zipcode (str): zipcode value.
    Returns:
        True if valid False if zipcode is invalid.
    """
    if len(zipcode) < 5:
        Flag = False
    else:
        Flag = True
    return Flag


def check_date(date):
    """Check the validity of the date value.

    Args:
        date (str): date value.
    Returns:
        True if valid False if date is invalid.
    """
    Flag = True
    try:
        datetime.strptime(date, '%m%d%Y')
    except ValueError:
        Flag = False
    return Flag


def read_line(filename='input/itcont.txt'):
    """
    iterate each line of document as a python generator and returns the
    process tuple if requirement are met.

    Args:
        filename (str): name and path of the input file.

    Yields:
        tuple: tuple of length four.
    """

    with open(filename) as f:
        for line in f:
            try:
                processed_line = process_line(line)
            except:
                continue
            yield processed_line


def string_to_int(string):
    """Converts the string to integer and removes the symbols from strings.
    Args:
        String (str): String contribution amount.

    Returns:
        int: Integer value .

    """

    non_decimal = re.compile(r'[^\d.]+')
    return int(non_decimal.sub('', string))


def write_zipcode(record,
                  output_filename='output/medianvals_by_zip.txt'):
    """
    Writes one line to the medianvals_by_zip.txt

    Args:
        output_filename (str): name and path of output filename.
        record (record object):
    """

    with open(output_filename, 'a') as output:
        output.writelines('{}|{}|{}|{}|{}\n'.format(record.ID,
                                                    record.zipcode,
                                                    record.median,
                                                    record.count,
                                                    record.total))


def write_date(date_shelve, output_filename='output/medianvals_by_date.txt'):
    """
    Writes one line to the medianvals_by_date.txt

    Args:
        output_filename (str): name and path of output filename.
        record (record object):
    """
    with open(output_filename, 'a') as output:
        keys = date_shelve.keys()
        sorted_keys = sort_key_by_date(keys)
        for key in sorted_keys:
            record = date_shelve[key]
            output.writelines('{}|{}|{}|{}|{}\n'.format(record.ID,
                                                        record.date,
                                                        record.median,
                                                        record.count,
                                                        record.total))


def date_to_string(date):
    """Converts datetime object to string.

    Args:
        date: datetiem object
    Returns:
        string format
    """

    return datetime.strftime(date, '%m%d%Y')


def sort_key_by_date(keys):
    """sort the dictionary key value first by id value and secondly by
    the date.

    Args:
        keys (list): list of key strings.
    Returns:
        sorted keys.
    """
    list_of_keys = []
    for key in keys:
        id, date = key.split(' ')
        list_of_keys.append([id, datetime.strptime(date, '%m%d%Y')])
    sorted_list = sorted(list_of_keys)
    dates = [date_to_string(x[1]) for x in sorted_list]
    ids = [x[0] for x in sorted_list]
    sorted_list = [id + ' ' + date for id, date in zip(ids, dates)]
    return sorted_list


def rounder(number):
    """
    round up if anything greater equall to $0.5 and round down otherwise.

    Args:
        number(float)
    Returns:
        rounded integer value.
    """
    if type(number) is int:
        output = number
    else:
        frac, whole = modf(number)
        output = int(whole) if frac < 0.5 else int(whole + 1)
    return output


class ZipRecord(object):
    """Stroring contribution information for contributions at each zipcode."""

    def __init__(self, line):
        """
        Args:
            line (str): One line of document.
        """

        ID, zipcode, date, amount = line
        self.zipcode = zipcode
        self.ID = ID
        self.key = ID + ' ' + zipcode
        self.amount = [string_to_int(amount)]

    def __call__(self, line):
        self.amount.append(string_to_int(line[-1]))

    @property
    def median(self):
        """int: Running median."""
        return rounder(median(self.amount))

    @property
    def count(self):
        """int: number of donations."""
        return len(self.amount)

    @property
    def total(self):
        """int: total donation amount."""
        return rounder(sum(self.amount))


class DateRecord(object):
    """Storing contribution information for contributions on each date."""
    def __init__(self, line):
        """
        Args:
            line (str): One line of document.
        """

        ID, zipcode, date, amount = line
        self.ID = ID
        self.key = ID + ' ' + date
        self.date = date
        self.amount = [string_to_int(amount)]
        self.output = None

    def __call__(self, line):
        self.amount.append(string_to_int(line[-1]))

    @property
    def median(self):
        """int: Median contribution."""
        return rounder(median(self.amount))

    @property
    def count(self):
        """int: Number contribution."""
        return len(self.amount)

    @property
    def total(self):
        """int: Total contribution amount."""
        return rounder(sum(self.amount))


class Shelve(object):
    """This object opens and updates the python shelve for storing data."""

    def __init__(self, zipcode_output_path,
                 cache_size=1000):
        """
        Args:
            zipcode_output_path (str): path for storing python shelves object
            on disk

            cache_size (int): Sync python shelves after desired number of
            dict object stored in memory.
            """
        self.zip_shelve = shelve.open('input/zip_shelve', writeback=True)
        self.date_shelve = shelve.open('input/date_shelve', writeback=True)
        self.zip_bloom_list = set()
        self.temp = []
        self.count = 0
        self.zipcode_output_path = zipcode_output_path
        self.cache_size = cache_size

    def update_zip_record(self, record, line):
        """Updates the object storing information for each CMTE_ID and zipcode
        combination

        Args:
            record: ZipRecord object.
            line (tuple): Tuple of record.
        """
        if check_zipcode(line[1]):
            if record.key not in self.zip_shelve.keys():
                self.zip_shelve[record.key] = record
            else:
                record = self.zip_shelve[record.key]
                record(line)
            write_zipcode(record, output_filename=self.zipcode_output_path)

    def update_date_record(self, record, line):
        """Updates the object storing information for each CMTE_ID and date
        combination.

        Args:
            record: DateRecord object.
            line (tuple): Tuple of record.
        """
        if check_date(line[2]):
            if record.key not in self.date_shelve.keys():
                self.date_shelve[record.key] = record
            else:
                record = self.date_shelve[record.key]
                record(line)

    def __call__(self, line):
        zip_record = ZipRecord(line)
        date_record = DateRecord(line)
        self.update_zip_record(zip_record, line)
        self.update_date_record(date_record, line)
        self.count += 1
        if self.count == self.cache_size:
            self.zip_shelve.sync()
            self.date_shelve.sync()
            self.count = 0

    def close(self):
        """Close shelve objects."""

        self.date_shelve.close()
        self.zip_shelve.close()


if __name__ == '__main__':
    start = time()

    parser = argparse.ArgumentParser()
    parser.add_argument('input', help='input file path')
    parser.add_argument('zipcode_path', help='zipcode output file path')
    parser.add_argument('date_path', help='date output file path')
    args = parser.parse_args()

    if isfile(args.zipcode_path) or isfile(args.date_path):
        raise OSError

    my_shelve = Shelve(zipcode_output_path=args.zipcode_path,
                       cache_size=1000)
    for line in read_line(args.input):
        my_shelve(line)
    write_date(my_shelve.date_shelve, output_filename=args.date_path)
    my_shelve.close()

    print('elapsed time: {}'.format(time() - start))
