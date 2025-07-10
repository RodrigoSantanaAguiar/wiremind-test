import json
from typing import List
import pandas as pd

from hera.workflows import Workflow, Steps, script, Input, Output, Parameter, Artifact
from hera.shared import global_config
from hera.workflows.models import ImagePullPolicy

#TODO: replace with actual image name
IMAGE_NAME = "image"
ARGO_HOST = "https://localhost:2746"

global_config.host = ARGO_HOST
global_config.image = IMAGE_NAME

# Tell Kubernetes the image already exists locally
# Make sure Kubernetes will not try to download the image
global_config.image_pull_policy = ImagePullPolicy.never


@script()
def prepare_data(data_path: str) -> dict:
    """
    Load data and filter confirmed tickets.
    :param data_path: The path to the parquet file.
    :return: A dictionary with the filtered data and a list of services.
    """

    # Import module here because this code will execute inside a container
    from data_pipeline.process import load_data, filter_confirmed_tickets
    import logging
    import json

    logging.info(f"Loading data from {data_path}...")
    raw_df = load_data(data_path)
    filtered_df = filter_confirmed_tickets(raw_df)
    services = filtered_df["service_number"].unique().tolist()

    logging.info(f"Loaded {filtered_df.shape[0]} confirmed tickets.")
    return {
        "filtered_data": filtered_df,   # Hera saves dataFrames as an artefact (Parquet)
        "services_list": services,      # Hera saves lists as a parameter (JSON)
    }


@script()
def process_service(filtered_data_chunk: Input[Artifact], service_id: str) -> Output[Artifact]:
    """
    Process a single service.
    :param filtered_data_chunk: The filtered data chunk. This is an artifact (Parquet) and will be automatically loaded.
    :param service_id: Service ID to process. This is a parameter (JSON) and will be automatically loaded.
    :return: An artifact (Parquet) with the intermediate results for the service.
    """
    from data_pipeline.process import process_intermediate_chunk
    import pandas as pd
    import logging

    logging.info(f"Processing service {service_id}...")

    filtered_df = filtered_data_chunk.load()

    service_chunk = filtered_df[filtered_df['service_number'] == int(service_id)]

    intermediate_result = process_intermediate_chunk(service_chunk)

    return intermediate_result


@script()
def merge_and_load(intermediate_results: Input[List[pd.DataFrame]]):
    """
    Aggregate intermediate results and save to Postgres.
    :param intermediate_results: Intermediate results to merge and load. This is a list of dataFrames and will be automatically loaded.
    :return:
    """
    from data_pipeline.process import perform_final_aggregation
    from data_pipeline.db import save_to_postgres
    import pandas as pd
    import logging

    logging.info("Merging and saving intermediate results...")

    if not intermediate_results:
        logging.warning("No intermediate results found, skipping...")
        return

    combined_df = pd.concat(intermediate_results, ignore_index=True)

    final_df = perform_final_aggregation(combined_df)

    save_to_postgres(final_df, table_name="service_demand_aggregation")
    logging.info("Intermediate results merged and saved to Postgres.")


with Workflow(
        generate_name="data-processing-pipeline-",
        entrypoint="pipeline_steps",
) as w:
    with Steps(name="pipeline_steps") as s:
        # Step 1: Prepare data
        prepare_task = prepare_data(
            arguments=[Parameter(name="data_path", value="/mnt/data/cayzn_tickets.parquet")]
        )

        # Step 2: Parallel processing
        process_task = process_service(
            with_param=prepare_task.get_parameter("services_list"),                 # Used for loops
            arguments={
                "filtered_data_chunk": prepare_task.get_artifact("filtered_data"),
                "service_id": "{{item}}",                                           # Get current loop's service_id
            },
        )

        # Step 3: Final aggregation
        merge_and_load(
            arguments=[process_task.get_artifact("intermediate_agg_artifact")]
        )
