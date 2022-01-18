#!/usr/bin/env python3

from pyspark.sql.functions import udf
import os
from pyspark import SparkFiles
import sys

mask_udf = udf(lambda fieldname : 'X' * (len(fieldname)-4) + fieldname[-4:] if (fieldname and len(fieldname) > 4) else fieldname )


def transformAndMask(spark, df_valid, sensitive_words):
    with open(SparkFiles.get(sensitive_words)) as f:
        fieldnames = f.read().split('\n')
        for fieldname in fieldnames:
            if (fieldname) :
                df_valid = func1(fieldname, df_valid)
    return df_valid

def func1(fieldname, df_valid):
    if fieldname in df_valid.schema.names:
        df_valid = df_valid.withColumn(fieldname, mask_udf(df_valid[fieldname]))
    return df_valid