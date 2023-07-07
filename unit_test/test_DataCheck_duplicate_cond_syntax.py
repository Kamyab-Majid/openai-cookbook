import json
import numpy as np
import pandas as pd
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col
from pyspark.sql.types import StringType, DateType, IntegerType, FloatType, DoubleType
from ..dq_utility import DataCheck


# Create DataFrame
@pytest.fixture
def spark():
    return SparkSession.builder.master("local").appName("DataCheckTest").getOrCreate()


@pytest.fixture
def df(spark):
    return spark.read.parquet("data/test_data.parquet")


@pytest.fixture
def datacheck_instance():
    spark = SparkSession.builder.master("local").appName("DataCheckTest").getOrCreate()
    df = spark.read.parquet("data/test_data.parquet")
    config_path = "s3://bedrock-test-bucket/config.json"
    file_name = "FSN001 - Fasenra (AstraZeneca) Detailed Reports"
    src_system = "innomar"
    data_check = DataCheck(df, spark, config_path, file_name, src_system)
    return data_check


def test_duplicate_cond_syntax(datacheck_instance, df):
    input_col = "Patient Number"
    expected_output = df.join(
        broadcast(df.groupBy(input_col).agg((f.count("*")).alias("Duplicate_indicator"))),
        on=input_col,
        how="inner",
    ).select(f.col("Duplicate_indicator") > 1)

    actual_output = datacheck_instance.duplicate_cond_syntax(input_col)

    assert actual_output.collect() == expected_output.collect()
