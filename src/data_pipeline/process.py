import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)

def load_data(path: str) -> pd.DataFrame:
    """
    Load data from a parquet file.
    :param path: Path to the parquet file
    :return: A pandas DataFrame
    """
    #TODO: add logs and try-except blocks
    logging.info(f"Loading data from {path}...")
    return pd.read_parquet(path)


def filter_confirmed_tickets(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter confirmed tickets from the dataframe.
    :param df: Raw dataframe containing all tickets.
    :return: A pandas DataFrame with only confirmed tickets.
    """
    logging.info("Filtering confirmed tickets...")
    filtered_df = df[df['is_confirmed'] == True].copy()

    if filtered_df.empty:
        logging.warning("No confirmed tickets found.")
    else:
        logging.info(f"{filtered_df.shape[0]} confirmed tickets found.")
    return filtered_df


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
    """
    Process a chunk of data (e.g., data for a single service) by applying
    temporal features and performing an intermediate aggregation.
    :param df_chunk: A dataFrame chunk containing confirmed tickets for one service.
    :return: A pandas dataFrame with partially aggregated results for the chunk.
    """
    if df_chunk.empty:
        logging.warning("Received an empty chunk, skipping...")
        return pd.DataFrame()

    logging.info(f"Processing chunk with {df_chunk.shape[0]} tickets...")

    # 1. Create temporal features for this specific chunk
    df_with_features = create_temporal_features(df_chunk)

    # 2. Define the keys for aggregation
    grouping_keys = [
        'od_origin_station_name',
        'od_destination_station_name',
        'departure_year',
        'departure_month',
        'departure_day',
        'departure_isoweekday',
        'timezone',
        'departure_time'
    ]

    # 3. Perform the intermediate aggregation.
    # Calculate counts and sums, which can be safely summed up again later.
    logging.info("Performing intermediate aggregation on the chunk...")
    intermediate_agg = df_with_features.groupby(grouping_keys).agg(
        total_demand=('ticket_key', 'count'),
        sum_of_prices=('price_vat_inc', 'sum'),
        ticket_count_for_mean=('price_vat_inc', 'count')
    ).reset_index()

    return intermediate_agg


def perform_final_aggregation(df_combined: pd.DataFrame) -> pd.DataFrame:
    """
    Receive the combined intermediate results from all chunks and
    perform the final aggregation to produce the final dataset.
    :param df_combined: A dataFrame containing the concatenated results from all parallel steps.
    :return: A pandas dataFrame matching the required output schema.
    """
    logging.info(f"Performing final aggregation on {df_combined.shape[0]} intermediate rows...")

    # The grouping keys are the same as before
    grouping_keys = [
        'od_origin_station_name',
        'od_destination_station_name',
        'departure_year',
        'departure_month',
        'departure_day',
        'departure_isoweekday',
        'timezone',
        'departure_time'
    ]

    # 1. Group the intermediate results and sum the partial sums and counts
    final_agg = df_combined.groupby(grouping_keys).sum().reset_index()

    # 2. Calculate the final 'mean_price'
    # Use a small epsilon to avoid division by zero if a count is somehow zero
    final_agg['mean_price'] = final_agg['sum_of_prices'] / (final_agg['ticket_count_for_mean'] + 1e-9)

    # 3. Select and reorder columns to match the final schema
    final_schema_columns = [
        'od_origin_station_name',
        'od_destination_station_name',
        'total_demand',
        'mean_price',
        'departure_year',
        'departure_month',
        'departure_day',
        'departure_isoweekday',
        'timezone',
        'departure_time'
    ]

    final_df = final_agg[final_schema_columns]
    logging.info(f"Final dataset created with {final_df.shape[0]} rows.")

    return final_df