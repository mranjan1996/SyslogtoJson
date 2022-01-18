#!/usr/bin/env python3
# This is a sample Python script.
# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
from pyspark import SparkConf, SparkContext
import pandas as pd
def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.
# Press the green button in the gutter to run the script.
with open('syslog_parser.py', 'rb+') as f:
    content = f.read()
    f.seek(0)
    f.write(content.replace(b'\r', b''))
    f.truncate()
if __name__ == '__main__':
    print_hi('PyCharm')
    conf = SparkConf().setAppName("SyslogParser app").setMaster("local")
    sc = SparkContext(conf=conf)
    data = [1, 2, 3, 4, 5]
    distData = sc.parallelize(data)
    print(distData.collect())
    print(type(distData))
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
