from datetime import datetime
from pyspark.sql import Row
spark = SparkSession(sc)
allTypes = sc.parallelize([Row(i=1, s="string", d=1.0, l=1,
    b=True, list=[1, 2, 3], dict={"s": 0}, row=Row(a=1),
    time=datetime(2014, 8, 1, 14, 1, 5))])
df = allTypes.toDF()
df.createOrReplaceTempView("allTypes")
spark.sql('SELECT  a.ca_state state,  count(*) cnt FROM customer_address a').collect()

df.rdd.map(lambda x: (x.i, x.s, x.d, x.l, x.b, x.time, x.row.a, x.list)).collect()