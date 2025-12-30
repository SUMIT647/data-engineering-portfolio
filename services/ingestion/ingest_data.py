"""
Data Ingestion Service
Monthly batch job to ingest customer transaction data from CSV files
Author: Data Engineering Portfolio
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path
import pandas as pd
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataIngestionService:
    """Handles monthly ingestion of customer transaction data"""
    
    def __init__(self, data_path='/opt/data'):
        self.data_path = Path(data_path)
        self.raw_path = self.data_path / 'raw'
        self.processed_path = self.data_path / 'processed'
        
        # Create directories if they don't exist
        self.raw_path.mkdir(parents=True, exist_ok=True)
        self.processed_path.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Initialized DataIngestionService with data_path: {data_path}")
    
    def validate_csv_file(self, file_path):
        """Validate CSV file exists and has required columns"""
        required_columns = [
            'Transaction_date', 'Transaction_ID', 'Gender', 'Age',
            'Marital_status', 'State_names', 'Segment', 'Employees_status',
            'Payment_method', 'Referral', 'Amount_spent'
        ]
        
        if not file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")
        
        # Read first few rows to check columns
        df_sample = pd.read_csv(file_path, nrows=5)
        missing_cols = set(required_columns) - set(df_sample.columns)
        
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        logger.info(f"CSV validation passed for: {file_path}")
        return True
    
    def clean_and_standardize(self, df):
        """Clean and standardize data types"""
        logger.info(f"Cleaning data... Initial shape: {df.shape}")
        
        # Create a copy to avoid warnings
        df = df.copy()
        
        # Convert transaction date to datetime
        df['Transaction_date'] = pd.to_datetime(df['Transaction_date'], errors='coerce')
        
        # Standardize column names (lowercase with underscores)
        df.columns = df.columns.str.lower().str.replace(' ', '_')
        
        # Handle missing values
        df['gender'] = df['gender'].fillna('Unknown')
        df['age'] = df['age'].fillna(df['age'].median()).astype(int)
        df['marital_status'] = df['marital_status'].fillna('Unknown')
        df['state_names'] = df['state_names'].fillna('Unknown')
        df['segment'] = df['segment'].fillna('Regular')
        df['employees_status'] = df['employees_status'].fillna('Unknown')
        df['payment_method'] = df['payment_method'].fillna('Unknown')
        df['referral'] = df['referral'].fillna('No').astype(str)  # Ensure string type
        df['amount_spent'] = pd.to_numeric(df['amount_spent'], errors='coerce').fillna(0)
        
        # Remove duplicates
        initial_count = len(df)
        df = df.drop_duplicates(subset=['transaction_id'], keep='first')
        duplicates_removed = initial_count - len(df)
        
        if duplicates_removed > 0:
            logger.info(f"Removed {duplicates_removed} duplicate transactions")
        
        # Remove invalid ages
        df = df[(df['age'] >= 18) & (df['age'] <= 120)]
        
        # Remove invalid amounts
        df = df[df['amount_spent'] >= 0]
        
        logger.info(f"Data cleaned. Final shape: {df.shape}")
        return df
    
    def partition_by_month(self, df):
        """Partition data by month for incremental processing"""
        logger.info("Partitioning data by month...")
        
        df['year'] = df['transaction_date'].dt.year
        df['month'] = df['transaction_date'].dt.month
        df['year_month'] = df['transaction_date'].dt.to_period('M')
        
        partitions = df.groupby('year_month')
        logger.info(f"Created {len(partitions)} monthly partitions")
        
        return partitions
    
    def save_monthly_partitions(self, df):
        """Save data partitioned by month"""
        partitions = self.partition_by_month(df)
        
        saved_files = []
        for year_month, data in partitions:
            year = data['year'].iloc[0]
            month = data['month'].iloc[0]
            
            # Create year directory
            year_path = self.raw_path / str(year)
            year_path.mkdir(exist_ok=True)
            
            # Convert timestamp to microseconds (compatible with Spark)
            data['transaction_date'] = data['transaction_date'].astype('datetime64[us]')
            
            # Save monthly file
            file_name = f"transactions_{year}_{month:02d}.parquet"
            file_path = year_path / file_name
            
            data.to_parquet(file_path, index=False, compression='snappy')
            saved_files.append(file_path)
            
            logger.info(f"Saved {len(data)} records to {file_path}")
        
        return saved_files
    
    def generate_ingestion_stats(self, df):
        """Generate statistics about ingested data"""
        stats = {
            'total_records': len(df),
            'date_range': {
                'min': df['transaction_date'].min().strftime('%Y-%m-%d'),
                'max': df['transaction_date'].max().strftime('%Y-%m-%d')
            },
            'total_revenue': float(df['amount_spent'].sum()),
            'avg_transaction': float(df['amount_spent'].mean()),
            'unique_customers': len(df['transaction_id'].unique()),
            'states_count': len(df['state_names'].unique()),
            'segments': df['segment'].value_counts().to_dict(),
            'payment_methods': df['payment_method'].value_counts().to_dict(),
        }
        
        return stats
    
    def ingest_csv(self, csv_file_name='customer_spending_1M_2018_2025.csv'):
        """Main ingestion process"""
        try:
            logger.info(f"Starting data ingestion for {csv_file_name}")
            start_time = datetime.now()
            
            # Locate CSV file
            csv_path = self.data_path / csv_file_name
            
            # Validate CSV
            self.validate_csv_file(csv_path)
            
            # Read CSV in chunks for memory efficiency
            logger.info(f"Reading CSV file: {csv_path}")
            df = pd.read_csv(csv_path, dtype={'Transaction_ID': str})
            logger.info(f"Loaded {len(df)} records from CSV")
            
            # Clean and standardize
            df = self.clean_and_standardize(df)
            
            # Save monthly partitions
            saved_files = self.save_monthly_partitions(df)
            
            # Generate statistics
            stats = self.generate_ingestion_stats(df)
            
            # Save ingestion metadata
            metadata = {
                'ingestion_date': datetime.now().isoformat(),
                'source_file': csv_file_name,
                'files_created': len(saved_files),
                'statistics': stats,
                'execution_time_seconds': (datetime.now() - start_time).total_seconds()
            }
            
            metadata_path = self.processed_path / f"ingestion_metadata_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            import json
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2, default=str)
            
            logger.info(f"Ingestion completed successfully in {metadata['execution_time_seconds']:.2f} seconds")
            logger.info(f"Statistics: {stats}")
            
            return metadata
            
        except Exception as e:
            logger.error(f"Ingestion failed: {str(e)}", exc_info=True)
            raise


def main():
    """Main entry point for ingestion service"""
    try:
        # Initialize service
        data_path = os.getenv('DATA_PATH', '/opt/data')
        ingestion_service = DataIngestionService(data_path)
        
        # Run ingestion
        csv_file = 'customer_spending_1M_2018_2025.csv'
        metadata = ingestion_service.ingest_csv(csv_file)
        
        logger.info("=" * 80)
        logger.info("INGESTION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total Records: {metadata['statistics']['total_records']:,}")
        logger.info(f"Date Range: {metadata['statistics']['date_range']['min']} to {metadata['statistics']['date_range']['max']}")
        logger.info(f"Total Revenue: ${metadata['statistics']['total_revenue']:,.2f}")
        logger.info(f"Avg Transaction: ${metadata['statistics']['avg_transaction']:.2f}")
        logger.info(f"Files Created: {metadata['files_created']}")
        logger.info(f"Execution Time: {metadata['execution_time_seconds']:.2f} seconds")
        logger.info("=" * 80)
        
        return 0
        
    except Exception as e:
        logger.error(f"Main execution failed: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
