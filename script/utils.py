import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

import datetime
from init import spark

def load_config():
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", "AKIAQXSPFBEX3T2PAJDV")
    hadoop_conf.set("fs.s3a.secret.key", "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")

def str_to_date(date_str):
    if date_str is None:
        date_str = '1970-01-01 00:00:00'
    return datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')