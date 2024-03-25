from py_func.kaggle_ops import download_datasets, init_on_kaggle
from py_func.spark_ops import sparkTest


if __name__ == '__main__':

    # init Kaggle API and Pull Data to Localhost
    init_on_kaggle('testexerciseuser', '1ab7db4952d8bca38e7357e5d4dbdd35')
    download_datasets('coronavirus-covid19-tweets-early-april', 'csv')

    spark = sparkTest(data_path='csv')
    
    # Derived Data for Q1
    sdf_derived_q1 = spark.derived_df_q1()
    sdf_derived_q1.show()

    # Derived Data for Q2
    sdf_derived_q2 = spark.derived_df_q2()
    sdf_derived_q2.show()

    # Derived Data for Q3
    sdf_derived_q3 = spark.derived_df_q3()
    sdf_derived_q3.show()
