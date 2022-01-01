pyspark2 \
--master yarn \
--conf spark.ui.port=0 \
--conf spark.sql.warehouse.dir=/user/itv000155/warehouse
---
--------------------
spark. \
read. \
csv('/public/retail_db/orders',
header=True).show()
------
>>> empl=spark.read.\
csv('bapi/empl.csv',header=True,inferSchema=True)
>>> empl.show()
type(empl)
---
empl.show(5,truncate=False)
---
empl.printSchema()
----schema
test_schema=spark.read.\
csv('bapi/empl.csv',header=True,inferSchema=True).schema
>> type(test_schema) ----output
<class 'pyspark.sql.types.StructType'>
-------------
test_s= spark.read. \
schema(test_schema). \
csv("bapi/empl.csv",
header=True
)
--------------
>>> test_s.count()
14
>>> test_s.distinct().count()
p=test_s.count()
>>> print(p)
---
test_s.count() - test_s.distinct().count()
test_s.count() + test_s.distinct().count()
-----
>>> employees = [(1, "Scott", "Tiger", 1000.0, "united states"),
(2, "Henry", "Ford", 1250.0, "India"),
(3, "Nick", "Junior", 750.0, "united KINGDOM"),
(4, "Bill", "Gomes", 1500.0, "AUSTRALIA")
]
>>> print(employees)

column=('EMPNO','FNAME','LNAME','SAL','COUNTRY')

edf2=spark.createDataFrame(employees,column)
edf2.show()
print(edf2)
---------------explicitly define the data type for the lsit
edf2=spark.createDataFrame(employees,schema='''EMPNO INT,FNAME STRING,LNAME STRING,SAL FLOAT,COUNTRY STRING''')
edf2.printSchema()
----------------------
--Row Level Transformations or Projection of Data can be done using select, selectExpr, withColumn, drop on Data Frame
--We can pass the condition to filter or where either by using SQL Style or Programming Language Style
--By Key or Grouping Aggregations are typically performed using groupBy and then aggregate functions using agg
--We can sort the data in Data Frame using sort or orderBy
from pyspark.sql import *
---select
edf2.select('fname','lname').show()
--drop
edf2.drop('sal','lname')


---------pyspark.sql.functions.
/*String Manipulation Functions
Concatenating Strings - concat
Getting Length - length
Trimming Strings - trim,rtrim, ltrim
Padding Strings - lpad, rpad
Extracting Strings - split, substring
Date Manipulation Functions
Date Arithmetic - date_add, date_sub, datediff, add_months
Date Extraction - dayofmonth, month, year
Get beginning period - trunc, date_trunc
Numeric Functions - abs, greatest
Aggregate Functions - sum, min, max */

edf2.withColumn("Full_Name",concat('fname',lit(" "),'lname')).\
drop('fname','lname').show()
--
edf2.withColumn("Full_Name",concat('fname',lit(" "),'lname')).show(truncate=False).\
drop('fname','lname')
---
 edf2.select('Empno',concat('fname',lit(' '),'lname').alias('Full_name'),'Sal','Country').show(truncate=False)
--
edf2.selectExpr("empno","concat(fname, ' ', lname) as full_name","sal","COUNTRY").show(truncate=False)
---
----Overview of Spark Write APIs
empl=spark.read.\
schema(empl_schema).\
csv('bapi/empl.csv',header=True,inferSchema=True).show()

...
------------Processing Column Data
----Pre-defined Functions
l = [('X', )]
df = spark.createDataFrame(l, "dummy STRING")

from pyspark.sql.functions import current_date
df.select(current_date()).show()
df.printSchema
----------
from pyspark.sql.functions import col,lit
edf2.select(col('fname'),col('lname')).show()
edf2.groupBy('COUNTRY').count().show()
empl.groupBy('JOB').count().show()
edf2.orderBy('sal').show()
edf2.orderBy(col('sal').desc()).show()

---edf2.select(concat(col("fname"), ", ", col("lname"))).show()
---edf2.select(concat(edf2('fname'),' ',concat(edf2('lname')).alias('FullName')).show()
edf2.select(concat('fname',lit(' '),'lname').alias('Fullname')).show()
from pyspark.sql.functions import col, lower, upper, initcap, length
/*
from pyspark.sql.functions import col, lower, upper, initcap, length
from pyspark.sql.functions import *
from pyspark.sql import *
*/
--substring
df.select(substring(lit('Manoj Giri'),7,5)).show()
df.select(substring(lit('Manoj Giri'),-4,7)).show()
---split, explode
df.select(split(lit('Manoj Kumar Giri'),' ')).show()
df.select(split(lit('Manoj Kumar Giri'),' ').alias('Name')).show(truncate=False)
df.select(split(lit('Manoj Kumar Giri'),' ')[2]).show(truncate=False) ----we can pass the index [2] print Giri

 df.select(explode(split(lit('Manoj Kumar Giri'),' '))).show(truncate=False)----using explode we can print in a columnar format
 ---String Manipulation Functions
 ---withColumn & Lit & CONCAT
 edf2.withColumn('Full_name',concat('fname',lit(' '),'lname')).show()
 edf2.select(lpad(lit('fname'),10,'_')).show()
-------tril,ltrim,rtrim
l = [("   Hello.    ",) ]
df = spark.createDataFrame(l).toDF("dummy")
df.withColumn("ltrim", ltrim(col("dummy"))). \
withColumn("rtrim", rtrim(col("dummy"))). \
withColumn("trim", trim(col("dummy"))). \
show()
-------Date and Time - Overview
df.select(current_date()).show()
df.select(current_timestamp()).show()

---
datetimes = [("2014-02-28", "2014-02-28 10:00:00.123"),
("2016-02-29", "2016-02-29 08:08:08.999"),
("2017-10-31", "2017-12-31 11:59:59.123"),
("2019-11-30", "2019-08-31 00:00:00.000")]
datetimesDF = spark.createDataFrame(datetimes, schema="date STRING, time STRING")
datetimesDF.show()
---
 datetimesDF.\
... withColumn("datediff_date", datediff(current_date(), "date")). \
... withColumn("datediff_time", datediff(current_timestamp(), "time")). \
... show(truncate=False)

--
datetimesDF. \
    withColumn("months_between_date", round(months_between(current_date(), "date"), 2)). \
    withColumn("months_between_time", round(months_between(current_timestamp(), "time"), 2)). \
    withColumn("add_months_date", add_months("date", 3)). \
    withColumn("add_months_time", add_months("time", 3)). \
    show(truncate=False)
				




























