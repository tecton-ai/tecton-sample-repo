from datetime import datetime
from datetime import timezone
import pytest
import pandas


def write_table(spark, df, database: str, table: str):
    """Utility method for writing a testing dataframe to a hive table."""
    spark.sql(f"create database if not exists {database}")
    df.write.format("csv").saveAsTable(f"{database}.{table}", mode="overwrite")

def assert_frame_equivalent(actual: pandas.DataFrame, expected: pandas.DataFrame, sort_cols=[], schema=None, **kwargs):
    """Utility method for checking Pandas dataframe equivalence. Has some improvements over pandas.testing.assert_frame_equal."""
    if schema:
        actual = actual.astype(schema)
        expected = expected.astype(schema)

    if sort_cols:
        actual = actual.sort_values(sort_cols).reset_index(drop=True)
        expected = expected.sort_values(sort_cols).reset_index(drop=True)

    # This improves the readability of our error messages before doing a full data frame comparison
    # and also reorders columns which are expected by the assert_frame_equal call
    assert sorted(list(actual.columns)) == sorted(list(expected.columns))
    expected = expected[list(actual.columns)]

    try:
        pandas.testing.assert_frame_equal(actual, expected, **kwargs)
    except AssertionError as e:
        # The assert_frame_equal() error messages are not very good. Format our own.
        actual_string = f"Actual=\n{actual.to_string()}"
        expected_string = f"Expected=\n{expected.to_string()}"
        # Exception chaining. https://peps.python.org/pep-3134/#explicit-exception-chaining
        msg = f"Dataframes are not equivalent.\n\n{actual_string}\n\n{expected_string}\ndtypes\n{actual.dtypes}\n{expected.dtypes}"
        raise AssertionError(msg) from e