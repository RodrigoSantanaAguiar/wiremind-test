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


