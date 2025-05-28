import streamlit as st
import boto3
import pandas as pd
from io import StringIO

bucket_name = "transformed-s3-global-patners"
prefix = "transformed_data/"

s3 = boto3.client("s3")

response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
csv_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]

st.title("S3 CSV Viewer")

selected_file = st.selectbox("Choose a file", csv_files)

if selected_file:
    # Read the selected file
    obj = s3.get_object(Bucket=bucket_name, Key=selected_file)
    body = obj['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(body))

    # Display the data
    st.write(f"### Data from `{selected_file}`")
    st.dataframe(df)
