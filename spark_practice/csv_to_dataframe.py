from pyspark.sql import SparkSession
import os


def repo_path(relative_path):
    current_dir = os.path.dirname(__file__)
    # relative_path = ".././data/spark_in_action_data/books.csv"
    absolute_file_path = os.path.join(current_dir, relative_path)
    return absolute_file_path

relative_path = ".././data/spark_in_action_data/books.csv"
path_file = repo_path(relative_path)
print(path_file)
# print(current_dir)

# /home/rohitgupta/BigData_projects/pyspark_experiments/data/spark_in_action_data/books.csv


# Creates a session on a local master
session = SparkSession.builder.appName("CSV to Dataset").master("local[*]").getOrCreate()

# Reads a CSV file with header, called books.csv, stores it in a dataframe
df = session.read.csv(header=True, inferSchema=True, path=path_file)

df.show(5)

session.stop()