from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import kaggle
import sqlite3

from airflow.utils.dates import days_ago

default_path = "/home/luccidx/T1/"

def extract_data():
    print("Extracting data from Kaggle")
    dataset = "arslaan5/global-data-gdp-life-expectancy-and-more"
    kaggle.api.dataset_download_files(dataset, path=default_path, unzip=True)
    print("Data extracted successfully")

def select_data():
    print("Reading data from CSV file")
    data = pd.read_csv(default_path + "country_data.csv")
    print("Data read successfully")
    print("Selecting important columns")
    important_columns = ['name', 'capital', 'surface_area', 'gdp', 'gdp_per_capita', 'population', 'imports', 'exports',
                         'gdp_growth', 'life_expectancy_male', 'life_expectancy_female']
    important_data = data[important_columns]
    print("Important columns selected")
    print("Saving important data to CSV file")
    important_data.to_csv(default_path + "important_country_data.csv", index=False)

def remove_duplicates():
    print("Removing duplicates from data")
    data = pd.read_csv(default_path + "important_country_data.csv")
    data.drop_duplicates(inplace=True)
    print("Duplicates removed")
    print("Saving data to CSV file")
    data.to_csv(default_path + "important_country_data.csv", index=False)

def cal_avg_life_expectancy():
    print("Calculating average life expectancy")
    data = pd.read_csv(default_path + "important_country_data.csv")
    data['average_life_expectancy'] = (data['life_expectancy_male'] + data['life_expectancy_female']) / 2
    print("Average life expectancy calculated")
    print("Saving data to CSV file")
    data.to_csv(default_path + "important_country_data.csv", index=False)

def drop_missing_vals():
    print("Dropping rows with missing values")
    data = pd.read_csv(default_path + "important_country_data.csv")
    data.dropna(inplace=True)
    print("Rows with missing values dropped")
    print("Saving data to CSV file")
    data.to_csv(default_path + "important_country_data.csv", index=False)

def select_gdp_per_capita_above_10000():
    print("Selecting countries with GDP per capita above 10000")
    data = pd.read_csv(default_path + "important_country_data.csv")
    data = data[data['gdp_per_capita'] > 10000]
    print("Countries with GDP per capita above 10000 selected")
    print("Saving data to CSV file")
    data.to_csv(default_path + "important_country_data.csv", index=False)

def load_to_db():
    print("Loading data to SQLite database")
    data = pd.read_csv(default_path + "important_country_data.csv")
    conn = sqlite3.connect(default_path + "country_gdp_data.db")
    cursor = conn.cursor()
    create_table = """
                CREATE TABLE IF NOT EXISTS country_gdp_data_table (
                    name TEXT, 
                    capital TEXT, 
                    surface_area REAL, 
                    gdp REAL, 
                    gdp_per_capita REAL,
                    population REAL, 
                    imports REAL, 
                    exports REAL, 
                    gdp_growth REAL, 
                    life_expectancy_male REAL,
                    life_expectancy_female REAL, 
                    average_life_expectancy REAL
                )
            """
    cursor.execute(create_table)

    for row in data.itertuples(index=False):
        cursor.execute("""
                    INSERT INTO country_gdp_data_table 
                    (name, capital, surface_area, gdp, gdp_per_capita, population, imports, exports, gdp_growth, life_expectancy_male, life_expectancy_female, average_life_expectancy) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
        row.name, row.capital, row.surface_area, row.gdp, row.gdp_per_capita, row.population, row.imports, row.exports,
        row.gdp_growth, row.life_expectancy_male, row.life_expectancy_female, row.average_life_expectancy))

    conn.commit()
    conn.close()

# Defining DAG Arguments

default_args = {
    'owner': 'G Santosh Kumar',
    'start_date': days_ago(0),
    'email': ['girisantoshkumar1999@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Defining the DAGs

dag = DAG(
    'country_data_etl_dag',
    default_args=default_args,
    description='A DAG to perform ETL on country data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
)

# Defining the tasks

extract_data_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

select_data_task = PythonOperator(
    task_id='select_data',
    python_callable=select_data,
    dag=dag,
)

remove_duplicates_task = PythonOperator(
    task_id='remove_duplicates',
    python_callable=remove_duplicates,
    dag=dag,
)

cal_avg_life_expectancy_task = PythonOperator(
    task_id='cal_avg_life_expectancy',
    python_callable=cal_avg_life_expectancy,
    dag=dag,
)

drop_missing_vals_task = PythonOperator(
    task_id='drop_missing_vals',
    python_callable=drop_missing_vals,
    dag=dag,
)

select_gdp_per_capita_above_10000_task = PythonOperator(
    task_id='select_gdp_per_capita_above_10000',
    python_callable=select_gdp_per_capita_above_10000,
    dag=dag,
)

load_to_db_task = PythonOperator(
    task_id='load_to_db',
    python_callable=load_to_db,
    dag=dag,
)

# task pipeline

extract_data_task >> select_data_task >> remove_duplicates_task >> cal_avg_life_expectancy_task >> drop_missing_vals_task >> select_gdp_per_capita_above_10000_task >> load_to_db_task
