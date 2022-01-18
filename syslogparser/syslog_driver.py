from pyspark import SparkConf
from pyspark.sql import SparkSession
from syslogparser import syslog_mapper
import sys
import os

#def cleanup_error_file( ):
    #if os.path.isfile.(sys.argv[2], 'ERROR_UNKNOWN-AUDIT'):
        #print("File Exists")
        #os.rmdir.(sys.argv[2], 'ERROR_UNKNOWN-AUDIT')
    #else:
        #print("File doesn't exists")
if __name__ == '__main__':
    #cleanup_error_file()
    #print(sys.argv)
    #validate_arg_counts(sys.argv)
    conf = SparkConf()
    conf.set("fs.defaultFS", sys.argv[3])
    spark = SparkSession.builder.config(conf=conf).master("local").appName("SyslogMaskUtility").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    syslog_mapper_path = os.path.join(sys.argv[4], 'syslog_mapper.py')
    print(syslog_mapper_path)
    spark.sparkContext.addFile(syslog_mapper_path)
    syslog_parser_path = os.path.join(sys.argv[4], 'syslog_parser.py')
    spark.sparkContext.addFile(syslog_parser_path)
    syslog_MaskingUtility_path = os.path.join(sys.argv[4], 'syslog_maskingutility.py')
    sensitive_attributes_file = os.path.join(sys.argv[4], sys.argv[5])
    spark.sparkContext.addFile(sensitive_attributes_file)
    spark.sparkContext.addFile(syslog_MaskingUtility_path)
    syslog_events_path = os.path.join(sys.argv[1])
    raw_rdd = syslog_mapper.create_raw_rdd(syslog_events_path, sc)
    rdd_error_audit, rdd_error_activity, rdd_error_unknown = syslog_mapper.invalid_rdd(raw_rdd)
    rdd_error_audit.saveAsTextFile(os.path.join(sys.argv[2], 'ERROR_UNKNOWN-AUDIT'))
    rdd_error_activity.saveAsTextFile(os.path.join(sys.argv[2], 'ERROR_UNKNOWN-ACTIVITY'))
    rdd_error_unknown.saveAsTextFile(os.path.join(sys.argv[2], 'ERROR_UNKNOWN'))
    df_valid_activity, df_valid_audit, df_error = syslog_mapper.map_transform(raw_rdd, spark, sys.argv[5])
    error_output_path = os.path.join(sys.argv[2], 'error')
    df_error.write.format('json').mode('append').save(error_output_path)
    audit_output_path = os.path.join(sys.argv[2], 'Audit')
    df_valid_audit.write.format('json').mode('overwrite').save(audit_output_path)
    activity_output_path = os.path.join(sys.argv[2], 'Activity')
    df_valid_activity.write.format('json').mode('overwrite').save(activity_output_path)
    spark.stop()