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


