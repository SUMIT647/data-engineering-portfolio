"""
Spark Batch ETL and Feature Engineering
Quarterly batch processing for customer transaction data
Author: Data Engineering Portfolio
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SparkETLPipeline:
    """Spark-based ETL pipeline for feature engineering"""
    
    def __init__(self, app_name="CustomerTransactionETL"):
        """Initialize Spark session"""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info(f"Initialized Spark session: {app_name}")
    
    def read_raw_data(self, data_path='/opt/data/raw'):
        """Read monthly partitioned parquet files"""
        try:
            logger.info(f"Reading raw data from: {data_path}")
            # Match files at /opt/data/raw/YYYY/transactions_YYYY_MM.parquet
            df = self.spark.read.parquet(f"{data_path}/*/*.parquet")
            
            logger.info(f"Loaded {df.count()} records")
            logger.info(f"Schema: {df.printSchema()}")
            
            return df
        except Exception as e:
            logger.error(f"Failed to read raw data: {str(e)}")
            raise
    
    def create_customer_features(self, df):
        """Generate customer segment features"""
        logger.info("Creating customer segment features...")
        
        # Aggregate by customer segment and state
        customer_features = df.groupBy('segment', 'state_names').agg(
            F.count('transaction_id').alias('total_transactions'),
            F.sum('amount_spent').alias('total_amount_spent'),
            F.avg('amount_spent').alias('avg_transaction_amount'),
            F.min('amount_spent').alias('min_transaction_amount'),
            F.max('amount_spent').alias('max_transaction_amount'),
            F.stddev('amount_spent').alias('std_transaction_amount'),
            
            # Time-based metrics
            F.min('transaction_date').alias('first_transaction_date'),
            F.max('transaction_date').alias('last_transaction_date'),
            
            # Demographic metrics
            F.avg('age').alias('avg_age'),
            
            # Behavioral metrics
            F.avg(F.when(F.col('referral') == 'Yes', 1).otherwise(0)).alias('referral_rate')
        )
        
        # Calculate days active
        customer_features = customer_features.withColumn(
            'days_active',
            F.datediff(F.col('last_transaction_date'), F.col('first_transaction_date'))
        )
        
        # Calculate average days between transactions
        customer_features = customer_features.withColumn(
            'avg_days_between_transactions',
            F.col('days_active') / F.col('total_transactions')
        )
        
        # Add metadata
        customer_features = customer_features.withColumn(
            'feature_version', F.lit('v1')
        ).withColumn(
            'created_at', F.current_timestamp()
        )
        
        logger.info(f"Created {customer_features.count()} customer feature records")
        return customer_features
    
    def create_quarterly_features(self, df):
        """Generate quarterly aggregated features"""
        logger.info("Creating quarterly features...")
        
        # Extract year and quarter
        df = df.withColumn('year', F.year('transaction_date'))
        df = df.withColumn('quarter', F.quarter('transaction_date'))
        
        # Aggregate by quarter, segment, and state
        quarterly_features = df.groupBy('year', 'quarter', 'segment', 'state_names').agg(
            F.count('transaction_id').alias('quarterly_transactions'),
            F.sum('amount_spent').alias('quarterly_revenue'),
            F.avg('amount_spent').alias('avg_quarterly_transaction'),
            F.countDistinct('transaction_id').alias('unique_customers')
        )
        
        # Calculate growth rates using window functions
        window_spec = Window.partitionBy('segment', 'state_names').orderBy('year', 'quarter')
        
        quarterly_features = quarterly_features.withColumn(
            'prev_quarter_revenue',
            F.lag('quarterly_revenue', 1).over(window_spec)
        )
        
        quarterly_features = quarterly_features.withColumn(
            'revenue_growth_rate',
            F.when(
                F.col('prev_quarter_revenue').isNotNull(),
                (F.col('quarterly_revenue') - F.col('prev_quarter_revenue')) / F.col('prev_quarter_revenue')
            ).otherwise(0)
        )
        
        # Add metadata
        quarterly_features = quarterly_features.withColumn(
            'feature_version', F.lit('v1')
        ).withColumn(
            'created_at', F.current_timestamp()
        )
        
        # Drop intermediate columns
        quarterly_features = quarterly_features.drop('prev_quarter_revenue')
        
        logger.info(f"Created {quarterly_features.count()} quarterly feature records")
        return quarterly_features
    
    def create_payment_features(self, df):
        """Generate payment method features"""
        logger.info("Creating payment method features...")
        
        # Calculate total transactions for percentage calculation
        total_transactions = df.count()
        
        # Aggregate by payment method and segment
        payment_features = df.groupBy('payment_method', 'segment').agg(
            F.count('transaction_id').alias('total_transactions'),
            F.sum('amount_spent').alias('total_amount'),
            F.avg('amount_spent').alias('avg_transaction_amount')
        )
        
        # Calculate usage percentage
        payment_features = payment_features.withColumn(
            'usage_percentage',
            F.col('total_transactions') / F.lit(total_transactions)
        )
        
        # Add metadata
        payment_features = payment_features.withColumn(
            'feature_version', F.lit('v1')
        ).withColumn(
            'created_at', F.current_timestamp()
        )
        
        logger.info(f"Created {payment_features.count()} payment feature records")
        return payment_features
    
    def create_state_features(self, df):
        """Generate state-level geographic features"""
        logger.info("Creating state-level features...")
        
        # Aggregate by state
        state_features = df.groupBy('state_names').agg(
            F.countDistinct('transaction_id').alias('total_customers'),
            F.count('transaction_id').alias('total_transactions'),
            F.sum('amount_spent').alias('total_revenue'),
            F.avg('amount_spent').alias('avg_transaction_size'),
            F.avg('age').alias('avg_customer_age')
        )
        
        # Calculate average customer value
        state_features = state_features.withColumn(
            'avg_customer_value',
            F.col('total_revenue') / F.col('total_customers')
        )
        
        # Find top segment per state
        segment_window = Window.partitionBy('state_names').orderBy(F.desc('transaction_count'))
        
        top_segments = df.groupBy('state_names', 'segment').agg(
            F.count('transaction_id').alias('transaction_count')
        ).withColumn(
            'rank', F.row_number().over(segment_window)
        ).filter(F.col('rank') == 1).select('state_names', F.col('segment').alias('top_segment'))
        
        # Join top segment
        state_features = state_features.join(top_segments, 'state_names', 'left')
        
        # Find top payment method per state
        payment_window = Window.partitionBy('state_names').orderBy(F.desc('payment_count'))
        
        top_payments = df.groupBy('state_names', 'payment_method').agg(
            F.count('transaction_id').alias('payment_count')
        ).withColumn(
            'rank', F.row_number().over(payment_window)
        ).filter(F.col('rank') == 1).select('state_names', F.col('payment_method').alias('top_payment_method'))
        
        # Join top payment method
        state_features = state_features.join(top_payments, 'state_names', 'left')
        
        # Add metadata
        state_features = state_features.withColumn(
            'feature_version', F.lit('v1')
        ).withColumn(
            'created_at', F.current_timestamp()
        )
        
        logger.info(f"Created {state_features.count()} state feature records")
        return state_features
    
    def write_to_postgres(self, df, table_name, mode='append'):
        """Write DataFrame to PostgreSQL (placeholder - use Parquet for now)"""
        try:
            # Note: PostgreSQL JDBC driver setup would be needed for production
            # For now, we'll save to Parquet and log the intent
            logger.info(f"[INTENDED] Writing {df.count()} records to table: {table_name}")
            logger.info(f"[INFO] In production, configure PostgreSQL JDBC driver in Spark classpath")
            # Uncomment below when JDBC driver is properly configured
            # postgres_url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'postgres')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'feature_store')}"
            # postgres_properties = {
            #     "user": os.getenv('POSTGRES_USER', 'dataeng'),
            #     "password": os.getenv('POSTGRES_PASSWORD', 'dataeng123'),
            #     "driver": "org.postgresql.Driver"
            # }
            # df.write.jdbc(url=postgres_url, table=table_name, mode=mode, properties=postgres_properties)
            
        except Exception as e:
            logger.error(f"Failed to write to PostgreSQL: {str(e)}")
    
    def save_features_parquet(self, df, feature_name, output_path='/opt/data/features'):
        """Save features as Parquet files"""
        try:
            output_file = f"{output_path}/{feature_name}"
            logger.info(f"Saving features to: {output_file}")
            
            df.write.mode('overwrite').parquet(output_file)
            logger.info(f"Successfully saved {feature_name}")
            
        except Exception as e:
            logger.error(f"Failed to save parquet: {str(e)}")
            raise
    
    def log_pipeline_execution(self, pipeline_name, status, records_processed, execution_time):
        """Log pipeline execution to database"""
        try:
            log_data = [(
                pipeline_name,
                datetime.now(),
                status,
                records_processed,
                int(execution_time)
            )]
            
            log_schema = StructType([
                StructField("pipeline_name", StringType(), False),
                StructField("execution_date", TimestampType(), False),
                StructField("status", StringType(), False),
                StructField("records_processed", IntegerType(), True),
                StructField("execution_time_seconds", IntegerType(), True)
            ])
            
            log_df = self.spark.createDataFrame(log_data, schema=log_schema)
            self.write_to_postgres(log_df, 'pipeline_execution_log', mode='append')
            
        except Exception as e:
            logger.error(f"Failed to log execution: {str(e)}")
    
    def run_etl_pipeline(self):
        """Execute complete ETL pipeline"""
        try:
            logger.info("=" * 80)
            logger.info("STARTING SPARK ETL PIPELINE")
            logger.info("=" * 80)
            start_time = datetime.now()
            
            # Read raw data
            df = self.read_raw_data()
            total_records = df.count()
            
            # Create features
            logger.info("Generating features...")
            
            # 1. Customer features
            customer_features = self.create_customer_features(df)
            self.write_to_postgres(customer_features, 'customer_features', mode='overwrite')
            self.save_features_parquet(customer_features, 'customer_features')
            
            # 2. Quarterly features
            quarterly_features = self.create_quarterly_features(df)
            self.write_to_postgres(quarterly_features, 'quarterly_features', mode='overwrite')
            self.save_features_parquet(quarterly_features, 'quarterly_features')
            
            # 3. Payment features
            payment_features = self.create_payment_features(df)
            self.write_to_postgres(payment_features, 'payment_features', mode='overwrite')
            self.save_features_parquet(payment_features, 'payment_features')
            
            # 4. State features
            state_features = self.create_state_features(df)
            self.write_to_postgres(state_features, 'state_features', mode='overwrite')
            self.save_features_parquet(state_features, 'state_features')
            
            # Calculate execution time
            execution_time = (datetime.now() - start_time).total_seconds()
            
            # Log execution
            self.log_pipeline_execution(
                pipeline_name='spark_etl_quarterly',
                status='completed',
                records_processed=total_records,
                execution_time=execution_time
            )
            
            logger.info("=" * 80)
            logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
            logger.info(f"Total Records Processed: {total_records:,}")
            logger.info(f"Execution Time: {execution_time:.2f} seconds")
            logger.info("=" * 80)
            
            return {
                'status': 'success',
                'records_processed': total_records,
                'execution_time': execution_time
            }
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {str(e)}", exc_info=True)
            
            # Log failure
            try:
                self.log_pipeline_execution(
                    pipeline_name='spark_etl_quarterly',
                    status='failed',
                    records_processed=0,
                    execution_time=0
                )
            except:
                pass
            
            raise
        finally:
            self.spark.stop()


def main():
    """Main entry point"""
    try:
        pipeline = SparkETLPipeline()
        result = pipeline.run_etl_pipeline()
        return 0
    except Exception as e:
        logger.error(f"Main execution failed: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
