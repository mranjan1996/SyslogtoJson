#!/bin/bash
#==============================================================================================================================
# Creation Date : 20-10-2020
# Description   : Spark_submit script.
# File format   : Json.
# Location : /c/Users/mr250632/PycharmProjects/SyslogtoJson/syslogparser
# Location of the script : /c/Users/mr250632/PycharmProjects/SyslogtoJson/syslogparser
#==============================================================================================================================
#
start=`date +%s`
echo "getting fsystem"

ENV=${1:-local}
function prop {
    grep "${1}" env/${ENV}.properties|cut -d'=' -f2
}

echo "Starting spark-submit"
echo "Running from $pwd"

#/c/Spark/spark-3.0.1-bin-hadoop2.7/bin/spark-submit.cmd syslogparser/syslog_mapper.py "$(prop 'fsystem')" "$(prop 'input_path')"
#$SPARK_HOME/bin/spark-submit.cmd mapper/syslog_mapper.py "$(prop 'fsystem')" "$(prop 'input_path')"
$SPARK_HOME/bin/spark-submit.cmd mapper/syslog_mapper.py "$(prop 'input_path')" "&(prop 'output_path')" "$(prop 'fsystem')" "$(prop 'module_path')" "$(prop 'sensitivewords_file')"
echo "done"

end=`date +%s`
echo Execution time was `expr $end - $start` seconds.