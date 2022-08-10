import csv
import os
import sys
# Spark imports
from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
# Dask imports
import dask.bag as db
import dask.dataframe as df  # you can use Dask bags or dataframes
from csv import reader

'''
INTRODUCTION

The goal of this assignment is to implement a basic analysis of textual 
data using Apache Spark (http://spark.apache.org) and 
Dask (https://dask.org). 
'''

'''
DATASET

We will study a dataset provided by the city of Montreal that contains 
the list of trees treated against the emerald ash borer 
(https://en.wikipedia.org/wiki/Emerald_ash_borer). The dataset is 
described at 
http://donnees.ville.montreal.qc.ca/dataset/frenes-publics-proteges-injection-agrile-du-frene 
(use Google translate to translate from French to English). 
We will use the 2015 and 2016 data sets available in directory `data`.
'''

'''
HELPER FUNCTIONS

These functions are here to help you. Instructions will tell you when
you should use them. Don't modify them!
'''

# Initialize a spark session.
def init_spark():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark


# Useful functions to print RDDs and Dataframes.
def toCSVLineRDD(rdd):
    '''
    This function convert an RDD or a DataFrame into a CSV string
    '''
    a = rdd.map(lambda row: ",".join([str(elt) for elt in row]))\
           .reduce(lambda x, y: os.linesep.join([x, y]))
    return a + os.linesep


def toCSVLine(data):
    '''
    Convert an RDD or a DataFrame into a CSV string
    '''
    if isinstance(data, RDD):
        return toCSVLineRDD(data)
    elif isinstance(data, DataFrame):
        return toCSVLineRDD(data.rdd)
    return None

'''
Plain PYTHON implementation

To get started smoothly and become familiar with the assignment's 
technical context (Git, GitHub, pytest, GitHub actions), we will implement a 
few steps in plain Python.
'''

# Python answer functions
def count(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the number of trees (non-header
    lines) in the data file passed as first argument.
    Test file: tests/test_count.py
    Note: The return value should be an integer
    '''
    with open(filename) as f:
        data = reader(f)
        no_lines = len(list(data))
    return no_lines - 1


def parks(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks.py
    Note: The return value should be an integer
    '''
    park_count = 0
    with open(filename) as f:
        data = reader(f)
        for i, row in enumerate(data):
            if (i != 0) & (row[6] != ''):
                park_count += 1
    return park_count


def uniq_parks(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the list of unique parks where trees
    were treated. The list must be ordered alphabetically. Every element in the list must be printed on
    a new line.
    Test file: tests/test_uniq_parks.py
    Note: The return value should be a string with one park name per line
    '''
    parks = []
    with open(filename) as f:
        data = reader(f)
        for i, row in enumerate(data):
            if (i != 0) & (row[6] != ''):
                parks.append(row[6])
    unique_parks = sorted(set(parks))
    return '\n'.join(unique_parks) + '\n'


def uniq_parks_counts(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that counts the number of trees treated in each park
    and prints a list of "park,count" pairs in a CSV manner ordered
    alphabetically by the park name. Every element in the list must be printed
    on a new line.
    Test file: tests/test_uniq_parks_counts.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''
    parks = []
    with open(filename) as f:
        data = reader(f)
        for i, row in enumerate(data):
            if (i != 0) & (row[6] != ''):
                parks.append(row[6])
    tree_counts = {park: parks.count(park) for park in parks}
    sorted_tree_counts = sorted(tree_counts.items(), key=lambda x: x[0])
    formatted_tree_counts = [park + ',' + str(count) for (park, count) in sorted_tree_counts]
    return '\n'.join(formatted_tree_counts) + '\n'


def frequent_parks_count(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the list of the 10 parks with the
    highest number of treated trees. Parks must be ordered by decreasing
    number of treated trees and by alphabetical order when they have similar number.
    Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''
    parks = []
    with open(filename) as f:
        data = reader(f)
        for i, row in enumerate(data):
            if (i != 0) & (row[6] != ''):
                parks.append(row[6])
    tree_counts = {park: parks.count(park) for park in parks}
    top_parks = sorted(tree_counts.items(), key=lambda x: x[1], reverse=True)[0:10]
    formatted_top_parks = [park + ',' + str(count) for (park, count) in top_parks]
    return '\n'.join(formatted_top_parks) + '\n'


def intersection(filename1, filename2):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the alphabetically sorted list of
    parks that had trees treated both in 2016 and 2015. Every list element
    must be printed on a new line.
    Test file: tests/test_intersection.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''
    parks1 = []
    parks2 = []
    with open(filename1) as f1, open(filename2) as f2:
        data1 = reader(f1)
        for i, row in enumerate(data1):
            if (i != 0) & (row[6] != ''):
                parks1.append(row[6])
        data2 = reader(f2)
        for i, row in enumerate(data2):
            if (i != 0) & (row[6] != ''):
                parks2.append(row[6])
    inter = sorted(set(parks1).intersection(parks2))
    return '\n'.join(inter) + '\n'


'''
SPARK RDD IMPLEMENTATION

You will now have to re-implement all the functions above using Apache 
Spark's Resilient Distributed Datasets API (RDD, see documentation at 
https://spark.apache.org/docs/latest/rdd-programming-guide.html). 
Outputs must be identical to the ones obtained above in plain Python. 
However, all operations must be re-implemented using the RDD API, you 
are not allowed to simply convert results obtained with plain Python to 
RDDs (this will be checked). Note that the function *toCSVLine* in the 
HELPER section at the top of this file converts RDDs into CSV strings.
'''

# RDD functions

def count_rdd(filename):
    '''
    Write a Python script using RDDs that prints the number of trees
    (non-header lines) in the data file passed as first argument.
    Test file: tests/test_count_rdd.py
    Note: The return value should be an integer
    '''
    spark = init_spark()
    file = spark.sparkContext.textFile(filename)
    return file.count() - 1


def parks_rdd(filename):
    '''
    Write a Python script using RDDs that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks_rdd.py
    Note: The return value should be an integer
    '''
    spark = init_spark()
    file = spark.read.csv(filename, header=True).rdd
    park_count = file.map(lambda x: x[6])\
                     .filter(lambda x: x is not None)\
                     .count()
    return park_count


def uniq_parks_rdd(filename):
    '''
    Write a Python script using RDDs that prints the list of unique parks where
    trees were treated. The list must be ordered alphabetically. Every element
    in the list must be printed on a new line.
    Test file: tests/test_uniq_parks_rdd.py
    Note: The return value should be a CSV string
    '''
    spark = init_spark()
    file = spark.read.csv(filename, header=True).rdd
    unique_parks = file.map(lambda x: x[6])\
                       .filter(lambda x: x is not None)\
                       .distinct()\
                       .sortBy(lambda x: x)\
                       .collect()
    return '\n'.join(unique_parks) + '\n'


def uniq_parks_counts_rdd(filename):
    '''
    Write a Python script using RDDs that counts the number of trees treated in
    each park and prints a list of "park,count" pairs in a CSV manner ordered
    alphabetically by the park name. Every element in the list must be printed
    on a new line.
    Test file: tests/test_uniq_parks_counts_rdd.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''
    spark = init_spark()
    file = spark.read.csv(filename, header=True).rdd
    tree_counts = file.map(lambda x: (x[6], 1))\
                      .filter(lambda x: x[0] is not None)\
                      .reduceByKey(lambda x, y: x + y)\
                      .sortBy(lambda x: x[0])\
                      .map(lambda x: x[0] + ',' + str(x[1]))\
                      .collect()
    return '\n'.join(tree_counts) + '\n'


def frequent_parks_count_rdd(filename):
    '''
    Write a Python script using RDDs that prints the list of the 10 parks with
    the highest number of treated trees. Parks must be ordered by decreasing
    number of treated trees and by alphabetical order when they have similar
    number.  Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count_rdd.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''
    spark = init_spark()
    file = spark.read.csv(filename, header=True).rdd
    tree_counts = file.map(lambda x: (x[6], 1))\
                      .filter(lambda x: x[0] is not None)\
                      .reduceByKey(lambda x, y: x + y)\
                      .sortBy(lambda x: x[1], ascending=False)\
                      .map(lambda x: x[0] + ',' + str(x[1]))\
                      .take(10)
    return '\n'.join(tree_counts) + '\n'


def intersection_rdd(filename1, filename2):
    '''
    Write a Python script using RDDs that prints the alphabetically sorted list
    of parks that had trees treated both in 2016 and 2015. Every list element
    must be printed on a new line.
    Test file: tests/test_intersection_rdd.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''
    spark = init_spark()
    file1 = spark.read.csv(filename1, header=True).rdd
    file2 = spark.read.csv(filename2, header=True).rdd
    file1_trees = file1.map(lambda x: x[6])\
                       .filter(lambda x: x is not None)
    inter = file2.map(lambda x: x[6])\
                 .filter(lambda x: x is not None)\
                 .intersection(file1_trees)\
                 .sortBy(lambda x: x)\
                 .collect()
    return '\n'.join(inter) + '\n'


'''
SPARK DATAFRAME IMPLEMENTATION

You will now re-implement all the tasks above using Apache Spark's 
DataFrame API (see documentation at 
https://spark.apache.org/docs/latest/sql-programming-guide.html). 
Outputs must be identical to the ones obtained above in plain Python. 
Note: all operations must be re-implemented using the DataFrame API, 
you are not allowed to simply convert results obtained with the RDD API 
to Data Frames. Note that the function *toCSVLine* in the HELPER 
section at the top of this file also converts DataFrames into CSV 
strings.
'''

# DataFrame functions

def count_df(filename):
    '''
    Write a Python script using DataFrames that prints the number of trees
    (non-header lines) in the data file passed as first argument.
    Test file: tests/test_count_df.py
    Note: The return value should be an integer
    '''
    spark = init_spark()
    file = spark.read.csv(filename, header=True)
    return file.count()


def parks_df(filename):
    '''
    Write a Python script using DataFrames that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks_df.py
    Note: The return value should be an integer
    '''
    spark = init_spark()
    file = spark.read.csv(filename, header=True)
    park_count = file.select("Nom_parc")\
                     .where("Nom_parc is not null")\
                     .count()
    return park_count


def uniq_parks_df(filename):
    '''
    Write a Python script using DataFrames that prints the list of unique parks
    where trees were treated. The list must be ordered alphabetically. Every
    element in the list must be printed on a new line.
    Test file: tests/test_uniq_parks_df.py
    Note: The return value should be a CSV string
    '''
    spark = init_spark()
    file = spark.read.csv(filename, header=True)
    unique_parks = file.select("Nom_parc")\
                       .where("Nom_parc is not null")\
                       .distinct()\
                       .orderBy("Nom_parc")
    return toCSVLine(unique_parks)


def uniq_parks_counts_df(filename):
    '''
    Write a Python script using DataFrames that counts the number of trees
    treated in each park and prints a list of "park,count" pairs in a CSV
    manner ordered alphabetically by the park name. Every element in the list
    must be printed on a new line.
    Test file: tests/test_uniq_parks_counts_df.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''
    spark = init_spark()
    file = spark.read.csv(filename, header=True)
    tree_counts = file.select("Nom_parc")\
                      .where("Nom_parc is not null")\
                      .groupBy("Nom_parc")\
                      .count()\
                      .orderBy("Nom_parc")
    return toCSVLine(tree_counts)


def frequent_parks_count_df(filename):
    '''
    Write a Python script using DataFrames that prints the list of the 10 parks
    with the highest number of treated trees. Parks must be ordered by
    decreasing number of treated trees and by alphabetical order when they have
    similar number.  Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count_df.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    spark = init_spark()
    file = spark.read.csv(filename, header=True)
    tree_counts = file.select("Nom_parc")\
                      .where("Nom_parc is not null")\
                      .groupBy("Nom_parc")\
                      .count()\
                      .orderBy(desc("count"))\
                      .limit(10)
    return toCSVLine(tree_counts)


def intersection_df(filename1, filename2):
    '''
    Write a Python script using DataFrames that prints the alphabetically
    sorted list of parks that had trees treated both in 2016 and 2015. Every
    list element must be printed on a new line.
    Test file: tests/test_intersection_df.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''
    spark = init_spark()
    file1 = spark.read.csv(filename1, header=True)
    file2 = spark.read.csv(filename2, header=True)
    f1_parks = file1.select("Nom_parc")\
                    .where("Nom_parc is not null")\
                    .distinct()
    f2_parks = file2.select("Nom_parc") \
                    .where("Nom_parc is not null")\
                    .distinct()
    inter = f1_parks.join(f2_parks, f1_parks.Nom_parc == f2_parks.Nom_parc)\
                    .toDF("f1", "f2")\
                    .select("f1")\
                    .orderBy("f1")
    return toCSVLine(inter)

'''
DASK IMPLEMENTATION (bonus)

You will now re-implement all the tasks above using Dask (see 
documentation at http://docs.dask.org/en/latest). Outputs must be 
identical to the ones obtained previously. Note: all operations must be 
re-implemented using Dask, you are not allowed to simply convert 
results obtained with the other APIs.
'''

# Dask functions

def count_dask(filename):
    '''
    Write a Python script using Dask that prints the number of trees
    (non-header lines) in the data file passed as first argument.
    Test file: tests/test_count_dask.py
    Note: The return value should be an integer
    '''
    dd = df.read_csv(filename)
    return len(dd)


def parks_dask(filename):
    '''
    Write a Python script using Dask that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks_dask.py
    Note: The return value should be an integer
    '''
    dd = df.read_csv(filename, dtype={'Nom_parc': 'object'})
    return len(dd['Nom_parc'].dropna())


def uniq_parks_dask(filename):
    '''
    Write a Python script using Dask that prints the list of unique parks
    where trees were treated. The list must be ordered alphabetically. Every
    element in the list must be printed on a new line.
    Test file: tests/test_uniq_parks_dask.py
    Note: The return value should be a CSV string
    '''
    dd = df.read_csv(filename, dtype={'Nom_parc': 'object'})
    unique_parks = dd[['Nom_parc']].dropna()\
                                   .drop_duplicates()\
                                   .sort_values(by='Nom_parc')\
                                   .compute()
    return '\n'.join(unique_parks['Nom_parc'].tolist()) + '\n'


def uniq_parks_counts_dask(filename):
    '''
    Write a Python script using Dask that counts the number of trees
    treated in each park and prints a list of "park,count" pairs in a CSV
    manner ordered alphabetically by the park name. Every element in the list
    must be printed on a new line.
    Test file: tests/test_uniq_parks_counts_dask.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''
    dd = df.read_csv(filename, dtype={'Nom_parc': 'object'})
    tree_counts = dd.groupby('Nom_parc')\
                    .count()[['Nom_arrond']]\
                    .reset_index()\
                    .sort_values(by='Nom_parc')\
                    .compute()
    formatted = [p + ',' + str(c) for [p, c] in tree_counts.values.tolist()]
    return '\n'.join(formatted) + '\n'


def frequent_parks_count_dask(filename):
    '''
    Write a Python script using Dask that prints the list of the 10 parks
    with the highest number of treated trees. Parks must be ordered by
    decreasing number of treated trees and by alphabetical order when they have
    similar number.  Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count_dask.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''
    dd = df.read_csv(filename, dtype={'Nom_parc': 'object'})
    tree_counts = dd.groupby('Nom_parc')\
                    .count()[['Nom_arrond']] \
                    .reset_index()\
                    .sort_values(by='Nom_arrond', ascending=False)\
                    .head(10)
    formatted = [p + ',' + str(c) for [p, c] in tree_counts.values.tolist()]
    return '\n'.join(formatted) + '\n'


def intersection_dask(filename1, filename2):
    '''
    Write a Python script using Dask that prints the alphabetically
    sorted list of parks that had trees treated both in 2016 and 2015. Every
    list element must be printed on a new line.
    Test file: tests/test_intersection_dask.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''
    dd1 = df.read_csv(filename1, dtype={'Nom_parc': 'object', 'No_Civiq': 'object'})
    dd2 = df.read_csv(filename2, dtype={'Nom_parc': 'object', 'No_Civiq': 'object'})
    dd1_parks = dd1[['Nom_parc']].drop_duplicates()
    dd2_parks = dd2[['Nom_parc']].drop_duplicates()
    inter = df.merge(dd1_parks, dd2_parks, on=['Nom_parc'])\
              .dropna()\
              .sort_values(by='Nom_parc')\
              .compute()
    return '\n'.join(inter.Nom_parc.tolist()) + '\n'