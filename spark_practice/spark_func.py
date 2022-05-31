from pyspark.sql.functions import collect_set
from pyspark.sql import SparkSession

file_base  = "/home/rohitgupta/BigData_projects/pyspark_experiments/data/calendar/Calendar_base.csv"
spark = SparkSession.builder.appName("sparkExample").getOrCreate()
df_base = spark.read.format("csv").option("header", "true").load(file_base)


def sp_session(app_name):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark

def to_csv(file_name):
    df = spark.read.format("csv").option("header", "true").load(file_name)
    return df

def csv_to_sql_table(df, table_name):
    df_1 = df.createOrReplaceTempView(table_name)
    return df_1


df_1 = df_base.createOrReplaceTempView('cal_base')

sql = """SELECT calendarCode,
        json_agg(json_build_object(
	    'calendarCode', calendarcode,
		'schoolReference', schoolReference,
		'schoolYearTypeReference', schoolYearTypeReference,
		'calendarTypeDescriptor', calendartypedescriptor))
		FROM cal_base 
		GROUP BY calendarCode;
		 
		

	)
"""

df_sql = spark.sql(sql).show()
