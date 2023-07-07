import json
import numpy as np
import pandas as pd
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, DateType, IntegerType, FloatType, DoubleType
from pyspark.sql.functions import col
from ..dq_utility import DataCheck


# Create SparkSession
spark = SparkSession.builder.appName("Data Quality Unit Test").getOrCreate()

# Create DataFrame
df = spark.read.parquet("data/test_data.parquet")


# Create DataCheck instance
@pytest.fixture
def datacheck_instance():
    spark = SparkSession.builder.master("local").appName("DataCheckTest").getOrCreate()
    df = spark.read.parquet("data/test_data.parquet")
    config_path = "s3://bedrock-test-bucket/config.json"
    file_name = "FSN001 - Fasenra (AstraZeneca) Detailed Reports"
    src_system = "innomar"
    data_check = DataCheck(df, spark, config_path, file_name, src_system)
    return data_check


# Test null_cond_syntax function
def test_null_cond_syntax(datacheck_instance):
    input_col = "Patient Number"
    expected_output = (col(input_col) == "") | (col(input_col).isNull())
    assert datacheck_instance.null_cond_syntax(input_col) == expected_output
