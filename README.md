# Banking Data Analysis Using PySpark

## Project Overview

This project involves analyzing a dataset of banking transactions using PySpark. The primary objective is to perform data cleaning, handle missing values and outliers, and derive meaningful insights from the data. The dataset includes various attributes such as Account Number, Transaction ID, Transaction Type, Amount, Transaction Date, Branch Code, Currency, and more.

## Project Structure

- **data/**: Directory containing the raw data files.
- **scripts/**: Directory containing the PySpark scripts for data processing and analysis.
- **output/**: Directory for storing the final processed data and results.
- **README.md**: This file.

## Setup and Installation

### Prerequisites

- Python 3.6+
- PySpark
- AWS CLI (for S3 interactions, if needed)

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/banking-data-analysis.git
   cd banking-data-analysis
   ```

2. **Install required Python packages:**
   ```bash
   pip install pyspark
   ```

## Data Processing

### Step 1: Initialize Spark Session

Initialize a Spark session for the project:
```python
from pyspark.sql import SparkSession

sp = SparkSession.builder \
    .appName("Banking_Analysis_using_Pyspark") \
    .getOrCreate()

sp.conf.set("spark.sql.shuffle.partitions", "50")
```

### Step 2: Load Data

Load the datasets into DataFrames:
```python
df1 = sp.read.csv('data/transactions_part1.csv', header=True, inferSchema=True)
df2 = sp.read.csv('data/transactions_part2.csv', header=True, inferSchema=True)
```

### Step 3: Data Cleaning

1. **Fill Missing Values**: 
   - For numerical columns, fill missing values with the median.
   - For categorical columns, fill missing values with the mode.
   
   ```python
   from pyspark.sql.functions import col, count, when, mean, stddev, round as Fround, sum as Fsum, count as Fcount, desc as Fdesc

   # Fill missing values in df1
   numeric_columns = [col_name for col_name, dtype in df1.dtypes if dtype in ['int', 'double']]
   categorical_columns = [col_name for col_name, dtype in df1.dtypes if dtype == 'string']

   for column in numeric_columns:
       median_value = df1.approxQuantile(column, [0.5], 0.25)[0]
       df1 = df1.fillna({column: median_value})

   for column in categorical_columns:
       mode_value = df1.groupBy(column).count().orderBy('count', ascending=False).first()[0]
       df1 = df1.fillna({column: mode_value})
   
   # Repeat for df2
   ```

2. **Handle Outliers**: 
   Remove outliers based on the z-score method:
   ```python
   stats = df1.select(mean('Amount').alias('mean'), stddev('Amount').alias('stddev')).first()
   mean_value = stats['mean']
   stddev_value = stats['stddev']

   df1 = df1.filter((col('Amount') >= mean_value - 3 * stddev_value) & (col('Amount') <= mean_value + 3 * stddev_value))
   ```

### Step 4: Merge DataFrames

Merge `df1` and `df2` into a single DataFrame:
```python
df = df1.unionByName(df2)
```

### Step 5: Derive Insights

1. **Group and Aggregate Data**:
   ```python
   df_account_holders = df.groupBy('AccountHolder').agg(
       Fround(Fmean('LoanAmount'), 2).alias('Avg Loan Amount'),
       Fround(Fmean('CreditScore'), 2).alias('Avg Credit score'),
       Fround(Fmean('InterestRate'), 2).alias('Avg Interest Rate'),
       Fround(Fmean('TransactionTime'), 2).alias('Avg transaction Time')
   ).orderBy(Fdesc('Avg Loan Amount'))
   
   df_account_holders.show()
   ```

2. **Transaction Trends**:
   ```python
   df_trends = df.groupBy('TransactionYear', 'TransactionMonth').agg(
       Fround(Fsum('Amount'), 2).alias('TotalAmount'),
       Fcount('TransactionID').alias('TransactionCount')
   )
   
   df_trends.show()
   ```

### Step 6: Save Final DataFrame

Save the final DataFrame to an output directory:
```python
df.write.csv('output/final_dataframe.csv', header=True)
```

## Conclusion

This project demonstrates how to process and analyze banking transaction data using PySpark. The steps include data cleaning, handling missing values and outliers, merging datasets, deriving insights, and saving the final results.

For any questions or issues, please contact [yourname@domain.com].

---

