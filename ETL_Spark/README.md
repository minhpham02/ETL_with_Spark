Các công nghệ sử dụng:
 - Apache Spark 2.5.1
 - Airflow 2.4.1
 - Docker 4.26.1
 - Python 3.10.6
 - Java 11
 - Postgresql 15.5.1
 - Postgresjdbc 42.6.2

Hướng dẫn sử dụng:
 - Import thư viện PySpark, SparkSession
 - Cài đặt SparkSession bằng gói jar để kết nối với Postgresql
 - Đọc dữ liệu các bảng dữ liệu trong Postgresql bằng Spark.read
 - Lưu dữ liệu trong bảng vào dataframe
 - Xử lý dữ liệu trên dataframe trước khi chuyển dữ liệu vào data warehouse
 - Ghi dữ liệu từ dataframe vào data warehouse bằng Spark.write
 - Kết thúc phiên làm việc của Spark 
