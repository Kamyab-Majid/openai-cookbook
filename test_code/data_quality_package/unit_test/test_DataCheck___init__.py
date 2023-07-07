import pytest
from pyspark.sql import SparkSession
from data_quality_package.dq_utility import DataCheck

@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").appName("data_quality_test").getOrCreate()

@pytest.fixture
def df():
    return spark.read.parquet("data/test_data.parquet")

def test_datacheck_instance(spark, df):
    config_path = "s3://bedrock-test-bucket/config.json"
    file_name = "FSN001 - Fasenra (AstraZeneca) Detailed Reports" 
    src_system = "innomar"
    data_check = DataCheck(df, spark, config_path, file_name, src_system)
    assert isinstance(data_check, DataCheck)
    assert data_check.source_df is df
    assert data_check.spark is spark 
    assert data_check.config_path == config_path
    assert data_check.file_name == file_name
    assert data_check.src_system == src_system