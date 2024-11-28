import sys
from pyspark.sql import SparkSession

def process_data(date_arg):
    spark = SparkSession.builder.appName('GCPDataprocJob').enableHiveSupport().getOrCreate()
    
    date_arg = date_arg

    bucket = 'airflow_assignment_1'

    orders_data_path = f'gs://{bucket}/raw_data/orders_{date_arg}.csv'

    orders_df = spark.read.csv(orders_data_path,header=True,inferSchema=True)
    print(orders_df)
    completed_order_df = orders_df.filter(orders_df.order_status == 'Completed')

    pending_order_df = orders_df.filter(orders_df.order_status == 'Pending')

    failed_order_df = orders_df.filter(orders_df.order_status == 'Failed')
    
    completed_order = f"orders_completed_{date_arg}"

    pending_order = f"orders_completed_{date_arg}"

    failed_order = f"orders_completed_{date_arg}"

    completed_order_df.write \
        .mode("append") \
        .format("hive") \
        .insertInto(completed_order)
    
    spark.stop()

if __name__ == "__main__":
    date_arg = sys.argv[1]
    process_data(date_arg)