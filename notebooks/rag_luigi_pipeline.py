# --- File ini ditulis oleh Jupyter Notebook ---

import luigi
import os
import pandas as pd
from dotenv import load_dotenv
from datetime import date
from minio import Minio
from qdrant_client import QdrantClient, models
from langchain_openai import OpenAIEmbeddings
from luigi.contrib.minio import MinioTarget
