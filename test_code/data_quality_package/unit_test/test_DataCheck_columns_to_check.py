import pytest
from pyspark.sql import SparkSession
from data_quality_package.dq_utility import DataCheck

@pytest.fixture
def datacheck_instance():
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.parquet("data/test_data.parquet")
    config_path = "s3://bedrock-test-bucket/config.json"
    file_name = "FSN001 - Fasenra (AstraZeneca) Detailed Reports" 
    src_system = "innomar"
    data_check = DataCheck(df, spark, config_path, file_name, src_system) 
    return data_check

def test_columns_to_check(datacheck_instance):
    """
    Test columns_to_check function 
    """
    # Test when criteria column has NaN values
    datacheck_instance.rule_df = pd.DataFrame({"criteria": [1, np.nan, 3]})
    result = datacheck_instance.columns_to_check("criteria")
    expected = [0, 2]
    assert result == expected
    
    # Test when criteria column has no NaN values
    datacheck_instance.rule_df = pd.DataFrame({"criteria": [1, 2, 3]})
    result = datacheck_instance.columns_to_check("criteria")
    expected = [0, 1, 2]
    assert result == expected