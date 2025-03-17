from pyspark.sql import DataFrame

def assert_spark_frame_equal(left: DataFrame, right: DataFrame):
    """
    Test function to compare two pyspark dataframes rowwise and check if they are identical. 
 
    Parameters
    ----------
    left, right : DataFrame
         DataFrames to compare
 
    Raises
    ------
    AssertionError
        If test fails the rows that are not present in either datafram will be printed
    """
    try: 
          assert left.subtract(right).count() == 0 and right.subtract(left).count() == 0, "DataFrames are not equal"
    except AssertionError as e:
          diff_actual = left.exceptAll(right)
          diff_expected = right.exceptAll(left)

          print("Rows in left that are not in right:")
          diff_actual.show()
          print("Rows in right that are not in left:")
          diff_expected.show()
          raise
    print("DataFrames are equal")
    