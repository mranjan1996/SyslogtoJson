#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark import SparkConf
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from syslogparser import syslog_parser as sp
#import syslog_parser as sp
from syslogparser import syslog_maskingutility as mask_util
#import syslog_maskingutility as mask_util
import sys
import os
from syslogparser import invalidargumentnumbererror
import pyspark.sql.functions as F

def error_syslog(rdd_raw):
    rdd_error = rdd_raw.filter(lambda event: not (event.startswith('<128>1') or event.startswith('<134>1')))
    rdd_error_audit = rdd_error.filter(lambda event: sp.is_audit(event))
    rdd_error_activity = rdd_error.filter(lambda event: sp.is_activity(event))
    rdd_error_unknown = rdd_error.filter(lambda event: not (event.is_audit(event) or event.is_activity(event)))
    return rdd_error_audit, rdd_error_activity, rdd_error_unknown

def valid_syslog(rdd_raw):
    return rdd_raw.filter(lambda event: (event.startswith('<128>1') or event.startswith('<134>1')))

def create_raw_rdd(syslog_event_path, sc):
    return sc.textFile(syslog_event_path)

def invalid_rdd(rdd_raw):
    rdd_error_audit, rdd_error_activity, rdd_error_unknown = error_syslog(rdd_raw)
    return rdd_error_audit, rdd_error_activity, rdd_error_unknown

def map_transform(rdd_raw ,spark, sensitive_words):
    rdd_valid = valid_syslog(rdd_raw)
    rdd_transformed = rdd_valid.map(lambda event: sp.syslog_transform(event))
    #print(rdd_transformed.collect())
    df_transformed = rdd_transformed.toDF()
    print(df_transformed.schema)
    df_error = df_transformed.filter(col('eventType').startswith('ERROR_'))
    df_valid = df_transformed.filter(~col('eventType').startswith('ERROR_'))
    df_masked = mask_util.transformAndMask(spark, df_valid, sensitive_words)
    df_valid_activity = df_masked.where(df_masked.eventType == "ACTIVITY")
    df_valid_audit = df_masked.filter(df_masked.eventType == "AUDIT")
    return df_valid_activity, df_valid_audit, df_error

def validate_arg_counts(*args):
    if len(args) <= 7:
        raise invalidargumentnumbererror()