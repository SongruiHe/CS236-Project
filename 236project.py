from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql import SQLContext
import sys
import re
import os
import time

def union_recording(recording_path):
    dir = os.listdir(recording_path)
    recordings = []
    for d in dir:
        path = args[2] + '/' + d
        print(path)
        recordings.append(sc.textFile(path))

    record = sc.union(recordings)
    return record


def computePRCP_perDay(str):
    if str is 'A':  # 6
        return 4
    if str is 'B':  # 12
        return 2
    if str is 'C':  # 18
        return 1.3
    if str is 'D':  # 24
        return 1
    if str is 'E':  # 12
        return 2
    if str is 'F':  # 24
        return 1
    if str is 'G':  # 24
        return 1
    if str is 'H':
        return 0
    if str is 'I':
        return 0
    else:  # 99.99
        return 0


if __name__ == '__main__':
    start_time = time.time()
    args = sys.argv
    if len(args) != 4:
        print('Wrong args!\n')
        sys.exit(1)
    location_path = args[1]
    # location_path = '/home/jovyan/Locations/WeatherStationLocations.csv'
    recording_path = args[2]
    # recording_path = '/home/jovyan/Recordings'
    output_path = args[3]
    # output_path = '/home/jovyan/result.txt'

    # Spark environment initialize
    spark1 = SparkConf().setMaster("local").setAppName("236")
    sc = SparkContext(conf=spark1)
    spark = SparkSession \
        .builder \
        .appName('236') \
        .master("local") \
        .getOrCreate()

    # Group stations in US by state
    location = spark.read.csv(location_path).toPandas()  # Read csv to pandas dataframe
    pandasDF = location.loc[1:, ['_c0', '_c3', '_c4']]  # only take these columns
    sparkDF = spark.createDataFrame(pandasDF)  # convert the pandas dataframe to spark sql dataframe
    # CTRY==US and ST is non-empty and we only take column USAF and ST
    location_US = sparkDF.rdd.filter(lambda x: x[1] == 'US').filter(lambda x: x[2] != None).map(lambda x: (x[0], x[2]))
    location_US.take(5)

    # Get all the recording files
    record = union_recording(recording_path)
    # Remove the header to make it easy to process
    header = record.first()
    record = record.filter(lambda x: x != header).map(lambda x: re.findall('[\S]+', x)).map(
        lambda x: (x[0], (x[2][2:8], x[19])))
    # Join the record and the location
    temp1 = location_US.join(record)

    # Compute the PRCP of one day based on the character
    temp2 = temp1.map(lambda x: ((x[1][0], x[1][1][0]), float(x[1][1][1][0:4]) * computePRCP_perDay(x[1][1][1][4])))
    # Combine the records of same state and same day,compute their avg
    temp3 = temp2.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
        .mapValues(lambda x: round((x[0] / x[1]), 2))
    # Take the three columns we need
    temp3 = temp3.map(lambda x: ((x[0][0], x[0][1][2:4]), x[1]))

    # Compute the PRCP of a month(sum values that have the same key--key is state and month)
    temp4 = temp3.reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[1], False)
    # Compute the difference of max PRCP and min PRCP
    max_p = temp4.map(lambda x: (x[0][0], (x[0][1], x[1]))).groupByKey().map(
        lambda x: (x[0], sorted(x[1], key=lambda a: a[1], reverse=True))) \
        .map(lambda x: (x[0], x[1][0][1]))
    min_p = temp4.map(lambda x: (x[0][0], (x[0][1], x[1]))).groupByKey().map(
        lambda x: (x[0], sorted(x[1], key=lambda a: a[1], reverse=False))) \
        .map(lambda x: (x[0], x[1][0][1]))
    diff_result = max_p.union(min_p).reduceByKey(lambda x, y: round((x - y), 2)).sortBy(lambda x: x[1])

    # Compute the max month and the min month
    max_mon = temp4.map(lambda x: (x[0][0], (x[0][1], x[1]))).groupByKey().map(
        lambda x: (x[0], sorted(x[1], key=lambda a: a[1], reverse=True))) \
        .map(lambda x: (x[0], (x[1][0][0], round(x[1][0][1], 2))))
    min_mon = temp4.map(lambda x: (x[0][0], (x[0][1], x[1]))).groupByKey().map(
        lambda x: (x[0], sorted(x[1], key=lambda a: a[1], reverse=False))) \
        .map(lambda x: (x[0], (x[1][0][0], round(x[1][0][1], 2))))

    # Combine the result together, sorting by ascending order
    result = max_mon.join(min_mon).join(diff_result).sortBy(lambda x: x[1][1])
    final = result.collect()

    # Write the header of the result
    with open(output_path + '/' + 'result.txt', 'w') as file:
        file.write('{:<10s}  {:<10s} {:<10s} {:<10s} {:<10s} {:<10s}\r' \
                   .format('STATE', 'max_month', 'max_prcp', 'min_month', 'min_prcp', 'diff'))

    # Write the result
    for x in final:
        with open(output_path + '/' + 'result.txt', mode='a') as file:
            file.write('{:<10s}  {:<10s} {:<10s} {:<10s} {:<10s} {:<10s}\r' \
                       .format(str(x[0]), str(x[1][0][0][0]), str(x[1][0][0][1] / 4), str(x[1][0][1][0]),
                               str(x[1][0][1][1] / 4), str(x[1][1] / 4)))

    end_time = time.time()
    print('Total run time {} s'.format(start_time-end_time))