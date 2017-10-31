"Find-Political-Donors" Code Challenge
=======================================

Solution to Isnight Data Engineerign challenge.

The goal is to identify the zipcodes that are fertile for future donations and
dates which are more lucrative.

Solution
========

* Processing the data and remove the bad records.
* Store and update the records as new stream of data recieved.
* Since the median donation amount should be treated as stream of data, python
  shelve is used to store the data to disk in order to avoild caching a large
  dictionary in the memory.

Requirements
============
Python3 standard libraries.

Run Instructions
================
python3 ./src/find_political_donors.py ./input/itcont.txt ./output/medianvals_by_zip.txt ./output/medianvals_by_date.txt

