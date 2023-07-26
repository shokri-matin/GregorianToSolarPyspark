from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

def is_leap_year(df: dataframe, year_column_name: str) -> dataframe:

    """
    This function calculate leap years in gergorian format date.
    """
    return ((df[year_column_name] % 4 == 0) & ((df[year_column_name] % 100 != 0) | (df[year_column_name] % 400 == 0))))

def covert_gregorian_to_solar(df: dataframe, date_column: str):

    """
    This function creates new columns named: "ShamsiYear," "ShamsiMonth," "ShamsiDay."
    Date column must be timestamp type.
    The method which is used for creating steps described at "https://jostrans.org/issue17/art_darani.php"
    """

    df = df.withColumn("YearOfDays", dayofyear(df[date_column]))

    df = df.withColumn("IsLeapYear", )
    df = df.withColumn("IsLeapYear - 1", ((((df['year']-1) % 4 == 0) & (((df['year']-1) % 100 != 0) | ((df['year']-1) % 400 == 0)))))
    df = df.withColumn("DeyDayDiff", when(df["IsLeapYear - 1"], lit(11)).otherwise(10))
    df = df.withColumn("FarvardinDayDiff", lit(79))

    df = df.withColumn("C01", df["YearOfDays"] > df["FarvardinDayDiff"])

    df = df.withColumn("newYearOfDays", when(df["C01"], df["YearOfDays"] - df["FarvardinDayDiff"]).otherwise(df["YearOfDays"] + df["DeyDayDiff"]))
    df = df.withColumn("C02", df["newYearOfDays"] <= lit(186))
    df = df.withColumn("C03", df["newYearOfDays"] % 31 == 0)

    df = df.withColumn("ShamsiYear", when(df["C01"] & df["C02"] & df["C03"], df["year"] - lit(621)))
    df = df.withColumn("ShamsiMonth", when(df["C01"] & df["C02"] & df["C03"], df["newYearOfDays"]/lit(31)))
    df = df.withColumn("ShamsiDay", when(df["C01"] & df["C02"] & df["C03"], lit(31)))

    df = df.withColumn("ShamsiYear", when(df["C01"] & df["C02"] & ~df["C03"], df["year"] - lit(621)))
    df = df.withColumn("ShamsiMonth", when(df["C01"] & df["C02"] & ~df["C03"], (df["newYearOfDays"]/lit(31)) + lit(1)))
    df = df.withColumn("ShamsiDay", when(df["C01"] & df["C02"] & ~df["C03"], df["newYearOfDays"] % 31))

    df = df.withColumn("newYearOfDays", when(df["C01"] & ~df["C02"], df["newYearOfDays"] - lit(186)).otherwise(df["newYearOfDays"]))
    df = df.withColumn("C04", (df["newYearOfDays"] % 30 == 0))

    df = df.withColumn("ShamsiYear", when(df["C01"] & ~df["C02"] & df["C04"], df["year"] - lit(621)).otherwise(df["ShamsiYear"]))
    df = df.withColumn("ShamsiMonth", when(df["C01"] & ~df["C02"] & df["C04"], (df["newYearOfDays"]/lit(30)) + lit(6)).otherwise(df["ShamsiYear"]))
    df = df.withColumn("ShamsiDay", when(df["C01"] & ~df["C02"] & df["C04"], 30).otherwise(df["ShamsiDay"]))

    df = df.withColumn("ShamsiYear", when(df["C01"] & ~df["C02"] & ~df["C04"], df["year"] - lit(621)).otherwise(df["ShamsiYear"]))
    df = df.withColumn("ShamsiMonth", when(df["C01"] & ~df["C02"] & ~df["C04"], (df["newYearOfDays"]/lit(30)) + lit(7)).otherwise(df["ShamsiMonth"]))
    df = df.withColumn("ShamsiDay", when(df["C01"] & ~df["C02"] & ~df["C04"], df["newYearOfDays"] % 30).otherwise(df["ShamsiDay"]))

    return df

df = spark.createDataFrame([[2022,1,1],
                            [2016,9,3],
                            [2015,5,16],
                            [2014,5,16],
                            [2013,5,16],
                            [2012,5,16],
                            [2011,5,16],
                            [2010,5,16],
                            [2009,9,3],
                            [2000,9,3]],['year', 'month','date'])


df = df.withColumn('timestamp',to_date(concat_ws('-', df.year, df.month, df.date)))
df = covert_gregorian_to_solar(df, 'timestamp')
df.select("timestamp", "ShamsiYear","ShamsiMonth","ShamsiDay").show()

"""
+----------+----------+-----------+---------+
| timestamp|ShamsiYear|ShamsiMonth|ShamsiDay|
+----------+----------+-----------+---------+
|2022-01-01|      1400|         10|       11|
|2016-09-03|      1395|          6|       13|
|2015-05-16|      1394|          2|       26|
|2014-05-16|      1393|          2|       26|
|2013-05-16|      1392|          2|       26|
|2012-05-16|      1391|          2|       27|
|2011-05-16|      1390|          2|       26|
|2010-05-16|      1389|          2|       26|
|2009-09-03|      1388|          6|       12|
|2000-09-03|      1379|          6|       13|
+----------+----------+-----------+---------+
"""
