from data_quality_package.dq_utility import DataCheck

def test_conditional_check(datacheck_instance):
    # Test when the condition columns are in the dataframe 
    datacheck_instance.rule_df.loc["Patient Number", "conditional_columns"] = "Age, Gender"
    datacheck_instance.rule_df.loc["Patient Number", "conditional_column_value"] = ">=18, Male"
    datacheck_instance.rule_df.loc["Patient Number", "conditional_valuelist"] = ""
    datacheck_instance.conditional_check("Patient Number")
    assert "Patient Number conditional_check1" in datacheck_instance.error_columns

    # Test when one of the condition columns is missing 
    datacheck_instance.rule_df.loc["Patient Number", "conditional_columns"] = "Age, NotExists"
    datacheck_instance.rule_df.loc["Patient Number", "conditional_column_value"] = ">=18, __NOT__NULL__"
    datacheck_instance.conditional_check("Patient Number")
    assert datacheck_instance.sns_message == ['Column NotExists is not in report FSN001 - Fasenra (AstraZeneca) Detailed Reports while it is needed for conditional check']

    # Test when the condition values are float
    datacheck_instance.rule_df.loc["Patient Number", "conditional_columns"] = "Age, Weight"
    datacheck_instance.rule_df.loc["Patient Number", "conditional_column_value"] = ">=18, 100"
    datacheck_instance.conditional_check("Patient Number")
    assert "Patient Number conditional_check2" in datacheck_instance.error_columns

    # Test when the condition values contains NOT 
    datacheck_instance.rule_df.loc["Patient Number", "conditional_columns"] =  "Gender"
    datacheck_instance.rule_df.loc["Patient Number", "conditional_column_value"] = "__NOT__Male" 
    datacheck_instance.conditional_check("Patient Number")
    assert "Patient Number conditional_check3" in datacheck_instance.error_columns