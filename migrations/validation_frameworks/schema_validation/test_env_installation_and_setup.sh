#!/bin/bash
export PYTHONIOENCODING=utf8
sudo alternatives --set python /usr/bin/python3.6
export SPARK_HOME=/usr/lib/spark
export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.7-src.zip:$PYTHONPATH
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/build:$PYTHONPATH
sudo //usr/bin/pip3 install boto3
sudo //usr/bin/pip3 install pandas
sudo //usr/bin/pip3 install findspark
sudo //usr/bin/pip3 install pytest
sudo //usr/bin/pip3 install pytest-spark
sudo //usr/bin/pip3 install allure-pytest
sudo //usr/bin/pip3 install psycopg2-binary
sudo aws s3 sync s3://nurix-s3-bucket/qa_test /home/hadoop
sudo aws s3 sync s3://nurix-s3-bucket/qa_test/data_validation/jars //lib/spark/jars