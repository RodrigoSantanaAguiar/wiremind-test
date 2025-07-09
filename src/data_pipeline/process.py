import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)

def load_data(path: str) -> pd.DataFrame:
    """
    Load data from a parquet file.
    :param path: Path to the parquet file
    :return: A pandas DataFrame
    """
    return pd.read_parquet(path)


def create_temporal_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create temporal features from the service_departure_datetime column.
    :param df: Dataframe
    :return: A pandas DataFrame with temporal features extracted from service_departure_datetime.
    """

    # Avoid pandas' SettingWithCopyWarning
    df = df.copy()

    # Make sure the column is a datetime type
    df['service_departure_datetime'] = pd.to_datetime(df['service_departure_datetime'])

    # Extract date and time components from service_departure_datetime
    df['departure_year'] = df['service_departure_datetime'].dt.year
    df['departure_month'] = df['service_departure_datetime'].dt.month
    df['departure_day'] = df['service_departure_datetime'].dt.day

    # Return (0=Monday, ..., 6=Sunday).
    # Add 1 to get the ISO pattern (1-7)
    df['departure_isoweekday'] = df['service_departure_datetime'].dt.dayofweek + 1

    # Calculate the time after midnight in minutes
    df['departure_time'] = df['service_departure_datetime'].dt.hour * 60 + df['service_departure_datetime'].dt.minute

    return df


def process_intermediate_chunk(df_chunk: pd.DataFrame) -> pd.DataFrame:
    pass


def aggregate_by_od(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby(['origin', 'destination']).agg({
        'total_demand': 'count',
        'mean_price': 'sum'
    }
    )