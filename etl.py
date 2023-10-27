from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from datetime import datetime
import traceback
import sys

conf = SparkConf()
sc = SparkContext(conf=conf)
sc.setLogLevel("INFO")
spark = SparkSession.builder.enableHiveSupport().getOrCreate()

count = 0
error = ""
time_start = datetime.now()

ano_corte = f'{time_start.year - 6}'

SQL_TRUNCATE_TABLE = "TRUNCATE TABLE tb_name"
SQL_COUNT_TABLE = "SELECT COUNT(*) FROM tb_name"
SQL_FINISHED_ELT = f"""SELECT count(*) FROM table_name WHERE name IN ('name') AND TRUNC(time_end) = TO_DATE('{time_start.date()}', 'YYYY-MM-DD') AND quantity > 0 AND error IS NULL"""
sparkOracle = SparkSession.builder.getOrCreate()
oracle_driver = 'oracle.jdbc.driver.OracleDriver'
oracle_url = 'url'
oracle_user = 'user'
oracle_password = 'password'
oracle_properties = {
    "user": oracle_user,
    "password": oracle_password,
    "driver": "oracle.jdbc.driver.OracleDriver"
}

try:
    sparkOracle._jvm.java.lang.Class.forName(oracle_properties["driver"])
    connection = sparkOracle._jvm.java.sql.DriverManager.getConnection(
        oracle_url,
        oracle_properties["user"],
        oracle_properties["password"]
    )

    statement = connection.createStatement()
    result_set = statement.executeQuery(SQL_FINISHED_ELT)
    result = 0
    if result_set.next():
        result = result_set.getInt(1)

    if result >= 1:
        print(">>> Etl already ran today.")
        sys.exit(">>> Exiting ...")

    print(">>> Truncating table...")
    statement.close()
    statement = connection.createStatement()
    statement.execute(SQL_TRUNCATE_TABLE)
    statement.close()
    print(">>> Truncate done.")
    statement = connection.createStatement()
    rs = statement.executeQuery(SQL_COUNT_TABLE)
    rs.next()
    print(">>> Count: " + str(rs.getInt(1)))
    statement.close()
    connection.close()

 # query from the data source
    query = f"""

    """
    
    df = spark.sql(query)
    # insert of the data at the destnation
    df.write\
               .mode("append")\
               .format("jdbc")\
               .option("url", "url")\
               .option("driver", "oracle.jdbc.driver.OracleDriver")\
               .option("dbtable", "tb_name")\
               .option("user", "user")\
               .option("password", "password")\
               .option("batchsize", "100000")\
               .save()
    count = df.count()

except Exception as e:
    error_message = str(e)
    stack_trace = traceback.format_exc()
    error = "{" + f'"message": "{error_message}", "stack_trace:" "{stack_trace}"' + "}"
    if len(error) > 4000:
        error = error[:3990] + '..."}'

finally:
    if statement:
        statement.close()
    if connection:
        connection.close()

time_end = datetime.now()

log_df = sparkOracle.createDataFrame(
    [
        ("name", time_start, time_end, count, error),
    ],
    ["name", "time_start", "time_end", "quantity", "error"]
)
# insert on the log table
log_df.write\
        .mode("append")\
        .format("jdbc")\
        .option("url", "url")\
        .option("driver", "oracle.jdbc.driver.OracleDriver")\
        .option("dbtable", "tb_name")\
        .option("user", "user")\
        .option("password", "password")\
        .save()

if error:
    print(">>> Error message:")
    print(error_message)
    print(">>> Stack trace:")
    print(stack_trace)
    print(">>> An error has occurred while running the script. Log above.")
else:
    print(">>> Etl finished successfully.")

print(f'>>> Done. [started_at="{str(time_start)}", finished_at="{str(time_end)}", count={str(count)}]')
