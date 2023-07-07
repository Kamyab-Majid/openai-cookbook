import pytest
from data_quality_package import dq_utility
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

@pytest.fixture
def datacheck_instance(): 
    config_path = "s3://bedrock-test-bucket/config.json"
    file_name = "FSN001 - Fasenra (AstraZeneca) Detailed Reports" 
    src_system = "innomar"
    return dq_utility.DataCheck(source_df=df, spark_context=spark, config_path=config_path, file_name=file_name, src_system=src_system)

def test_add_error_col(datacheck_instance):
    """
    Test that add_error_col method adds an error column with the correct error message
    """
    datacheck_instance.add_error_col("Error Message", pyspark.sql.functions.col("condition"), "error_col")
    assert "error_col1" in datacheck_instance.source_df.columns
    assert datacheck_instance.source_df.filter(datacheck_instance.source_df["condition"] == True).first()["error_col1"] == "Error Message"

def test_add_error_col_no_condition(datacheck_instance):
    """
    Test that add_error_col method does not add an error column if condition is None
    """
    datacheck_instance.add_error_col("Error Message", None, "error_col")
    assert "error_col" not in datacheck_instance.source_df.columns

def test_add_error_col_increment(datacheck_instance):
    """
    Test that error_col_name increments correctly
    """
    datacheck_instance.add_error_col("Error Message", pyspark.sql.functions.col("condition"), "error_col")
    datacheck_instance.add_error_col("Error Message", pyspark.sql.functions.col("condition"), "error_col")
    assert "error_col1" in datacheck_instance.source_df.columns
    assert "error_col2" in datacheck_instance.source_df.columns