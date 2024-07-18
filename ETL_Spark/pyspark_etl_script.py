from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, dayofmonth, month, year, monotonically_increasing_id

spark = SparkSession.builder.appName("ETL")\
        .config('spark.jars.packages', 'org.postgresql:postgresql:42.6.2') \
        .getOrCreate()

# Dont Show warning only error
spark.sparkContext.setLogLevel("ERROR")

# Đọc dữ liệu từ bảng order_product
time_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://192.168.0.100:5432/e-commerce") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", "traianthai") \
    .option("password", "123123") \
    .option("dbtable", "public.\"order_product\"") \
    .load()

# Hiển thị dữ liệu
time_df.show()

# Thực hiện transform để chuyển đổi trường Time sang định dạng yyyy-mm-dd
time_df = time_df.select(to_date(col("Time")).alias("date"))
time_df.show()

# Tạo DataFrame mới dim_time từ trường Time
dim_time_df = time_df.select(
    dayofmonth(col("date")).alias("day"),
    month(col("date")).alias("month"),
    year(col("date")).alias("year")
).distinct()
dim_time_df.show()

# Thêm trường time_id bằng cách sử dụng hàm monotonically_increasing_id()
dim_time_df = dim_time_df.withColumn("time_id", monotonically_increasing_id())
dim_time_df.show()

# Ghi DataFrame dim_time vào bảng "Dim_Time" trong schema "public"
dim_time_df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://192.168.0.100:5432/data_warehouse") \
    .option("dbtable", "public.\"Dim_Time\"") \
    .option("user", "traianthai") \
    .option("password", "123123") \
    .mode("overwrite") \
    .save()

# Đóng SparkSession
spark.stop()
