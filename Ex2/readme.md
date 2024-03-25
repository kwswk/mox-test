This is my solution for Exercise 2:

## Virtual environment creation
You are suggested to run the ETL in an virtual environment and you could start a virtual encironment with the following command:
```
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Check if below packages are installed:
1. PySpark
2. kaggle


## Run Spark ETL in one command
```
python -m ex2
```

## Code explain
The code structure is simple:
1. [ex2.py](ex2.py) an entry point to run the ETL
2. [py_func](py_func) where stores ETL operation logic
   1. [py_func/kaggle_ops.py](py_func/kaggle_ops.py) Handles Kaggle API operations and Dataset preparation
   2. [py_func/spark_ops.py](py_func/spark_ops.py) Handles ALL ETL using PySpark

## My Answer to Exercise 2 / Q3
I would say it depends on business requirement that how frequent would they keep track the customer changes. The derived dataset produced in #1 stores the last screen name per day for all customers yet a customer could change multiple screen in a day. In my opion, I would start from source data as to handle potential change of requirements from business users give that computation resources is allowed.

## Sidenotes
- I didn't write any function to output spark dataframe as this is not stated in the question
- Please ignore files stored in [archived](archived)
  