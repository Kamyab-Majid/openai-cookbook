# data_quality_package/unit_test/test_dq_utility.py
import pytest 
from pyspark.sql import SparkSession
from data_quality_package.dq_utility import DataCheck

@pytest.fixture
def spark():
    return SparkSession.builder.getOrCreate()

@pytest.fixture 
def datacheck_instance(spark):
    config_path = "s3://bedrock-test-bucket/config.json"
    file_name = "FSN001 - Fasenra (AstraZeneca) Detailed Reports"
    src_system = "innomar"
    return DataCheck(spark, config_path, file_name, src_system)

def test_category_check(datacheck_instance):
    """Test the category_check method"""
    input_col = "column1"
    datacheck_instance.category_check(input_col)
    
    # Add assertions to check: 
    # - Input column is renamed to uppercase
    # - A join is performed on reference_columns
    # - An error column is added for invalid values 
    # - A warning is logged for missing reference columns

def test_file_check(datacheck_instance): 
    """Test the file_check method"""
    input_col = "column2"
    category_cond, category_error_msg = datacheck_instance.file_check(input_col) 
    
    # Add assertions to check:
    # - The reference file is read 
    # - Columns are renamed and standardized
    # - A join is performed on reference_columns
    # - category_cond and category_error_msg are returned  
    
# Add additional test methods for other functions in the DataCheck class