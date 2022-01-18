import pytest
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from syslogparser import syslog_mapper
from syslogparser.SensitiveWordsException import SensitiveWordsException


@pytest.fixture(scope='session')
def spark():
    spark = SparkSession.builder \
            .master("local") \
            .appName("syslog_parser") \
            .getOrCreate()
    return spark

@pytest.fixture()
def prepare_input(spark):
    input_syslog = [
        '<128>1 2021-02-09T00:01:01.958-08:00 anqal10abstc100.dca.diginsite.net ABS - Activity [RequestContext@12293 ReqCtx_appId="ABS" ReqCtx_bcId="07525" ReqCtx_bcIndex="25" ReqCtx_canonicalId="07525" ReqCtx_id="0e95521c-449a-4c15-8040-92df00066b3e" ReqCtx_onBehalfOf="07525" ReqCtx_region="qa" ReqCtx_transId="0e95521c-449a-4c15-8040-92df00066b3e"][scheduledTransferCount@12293 clientVersion="3.1.0" count="1" guId="f3b3ad2e-6aac-11eb-8683-005056a0a4c1" reportGenerationDateTime="02/09/2021 00:01:00.039 AM PST" scheduledDate="02/09/2021"]',
        '<121>1 2021-02-09T00:01:35.154-08:00 localhost.localdomain ABS - Activity [absRun@12293 absRunEvent="start" absRunId="0760a159-6aad-11eb-ae0a-005056a0f786" absRunTime="02/09/2021 00:01:35.154 AM PST" absRunType="ACCOUNT_ATTRIBUTE" guId="076f2027-6aad-11eb-81f7-005056a07b88" pageCount="0" subscriptionCount="0" userCount="0"][RequestContext@12293 ReqCtx_appId="ABS" ReqCtx_bcId="07527" ReqCtx_bcIndex="27" ReqCtx_canonicalId="07527" ReqCtx_country="US" ReqCtx_id="94da199b-61ac-44d5-aeed-939d715e8bca" ReqCtx_ipAddress="10.102.155.158" ReqCtx_locale="en_US" ReqCtx_offeringId="ABSalerts" ReqCtx_region="qa" ReqCtx_sessionId="0760a159-6aad-11eb-ae0a-005056a0f786" ReqCtx_timezone="America/Los_Angeles" ReqCtx_tzOffset="-0800" ReqCtx_userProduct="SRT" ReqCtx_userProductVersion="3.0"]'
    ]
    input_rdd = spark.sparkContext.parallelize(input_syslog)
    #input_df = input_rdd.toDF()
    return input_rdd

@pytest.fixture(scope="function")
def check_file_exist():
    sensitive_words = r"C:\Users\mr250632\PycharmProjects\SyslogtoJson\syslogparser\sensitivewords.properties"
    if not (os.path.exists(sensitive_words) and os.path.getsize(sensitive_words)):
        raise SensitiveWordsException("check_file_exist", sensitive_words)
    return  sensitive_words

def test_valid_syslog(prepare_input,spark):
    expected_syslog = ['<128>1 2021-02-09T00:01:01.958-08:00 anqal10abstc100.dca.diginsite.net ABS - Activity [RequestContext@12293 ReqCtx_appId="ABS" ReqCtx_bcId="07525" ReqCtx_bcIndex="25" ReqCtx_canonicalId="07525" ReqCtx_id="0e95521c-449a-4c15-8040-92df00066b3e" ReqCtx_onBehalfOf="07525" ReqCtx_region="qa" ReqCtx_transId="0e95521c-449a-4c15-8040-92df00066b3e"][scheduledTransferCount@12293 clientVersion="3.1.0" count="1" guId="f3b3ad2e-6aac-11eb-8683-005056a0a4c1" reportGenerationDateTime="02/09/2021 00:01:00.039 AM PST" scheduledDate="02/09/2021"]',]
    expected = spark.sparkContext.parallelize(expected_syslog).collect()
    actual = syslog_mapper.valid_syslog(prepare_input).collect()
    assert actual == expected

def test_map_transform(prepare_input, spark, check_file_exist):
    actual_valid, actual_error = syslog_mapper.map_transform(prepare_input, spark, check_file_exist)
    expected_list = [{'timeStamp': '2021-02-09T00:01:01.958-08:00', 'eventDate': '2021-02-09', 'eventType': 'ACTIVITY', 'eventName': 'scheduledTransferCount', 'ReqCtx_appId': 'ABS', 'ReqCtx_bcId': '07525', 'ReqCtx_bcIndex': '25', 'ReqCtx_canonicalId': '07525', 'ReqCtx_id': '0e95521c-449a-4c15-8040-92df00066b3e', 'ReqCtx_onBehalfOf': '07525', 'ReqCtx_region': 'qa', 'ReqCtx_transId': '0e95521c-449a-4c15-8040-92df00066b3e', 'clientVersion': '3.1.0', 'count': '1', 'guId': 'f3b3ad2e-6aac-11eb-8683-005056a0a4c1', 'reportGenerationDateTime': '02/09/2021 00:01:00.039 AM PST', 'scheduledDate': '02/09/2021'}]
    schema = StructType([StructField("ReqCtx_appId",StringType(),True),StructField("ReqCtx_bcId",StringType(),True),StructField("ReqCtx_bcIndex",StringType(),True),StructField("ReqCtx_canonicalId",StringType(),True),StructField("ReqCtx_id",StringType(),True),StructField("ReqCtx_onBehalfOf",StringType(),True),StructField("ReqCtx_region",StringType(),True),StructField("ReqCtx_transId",StringType(),True),StructField("clientVersion",StringType(),True),StructField("count",StringType(),True),StructField("eventDate",StringType(),True),StructField("eventName",StringType(),True),StructField("eventType",StringType(),True),StructField("guId",StringType(),True),StructField("reportGenerationDateTime",StringType(),True),StructField("scheduledDate",StringType(),True),StructField("timeStamp",StringType(),True)])
    expected = spark.createDataFrame(expected_list, schema)
    if actual_valid.schema == expected.schema:
        if actual_valid.count() == expected.count():
            if actual_valid.rdd.collect() == expected.rdd.collect():
                assert True
            else:
                 print("Actual RDD: {} \nExpected RDD: {} ".format(actual_valid.rdd.collect(), expected.rdd.collect()))
                 assert False, "Assertion failed for value mismatch"
        else:
            print("Actual row count: {} \nExpected row count: {} ".format(actual_valid.count(), expected.count()))
            assert False, "Assertion failed for row count"
    else:
        print("Actual schema: {} \nExpected schema: {} ".format(actual_valid.schema, expected.schema))
        assert False, "Assertion failed for schema"

