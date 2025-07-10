import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from typing import DataFrame

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def save_to_postgres(df: DataFrame, table_name: str):
    """
    Save a DataFrame to a specified table in the PostgreSQL database.
    It reads connection details from environment variables.

    :param df: The pandas DataFrame to save.
    :param table_name: The name of the table to create/replace.
    """
    # Read the environment password
    db_password = os.environ.get("DB_PASSWORD")
    if not db_password:
        logging.error("The variable DB_PASSWORD is not set.")
        raise ValueError("Data base password not found.")

    # Prepare the connection string
    db_host = "postgres-postgresql.db.svc.cluster.local"
    db_user = "postgres"
    db_name = "postgres"

    db_connection_str = f"postgresql://{db_user}:{db_password}@{db_host}:5432/{db_name}"

    try:
        logging.info(f"Connecting to database in '{db_host}'...")
        engine = create_engine(db_connection_str)

        with engine.connect() as connection:
            logging.info(f"Cleaning existing table '{table_name}' if any...")
            connection.execute(text(f"DROP TABLE IF EXISTS {table_name};"))

            logging.info(f"Saving {df.shape[0]} rows into '{table_name}'...").
            df.to_sql(table_name, engine, if_exists='replace', index=False)

            result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name};"))
            count = result.scalar()
            logging.info(f"Check: the table '{table_name}' contains {count} rows.")

    except Exception as e:
        logging.error(f"An error occurred while saving data to PostgreSQL: {e}")
        raise