import boto3
import urllib.parse
import os
import json
import openpyxl
import pandas as pd
from io import BytesIO
import logging
from botocore.config import Config

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
bedrock_agent_runtime = boto3.client(
    'bedrock-agent-runtime',
    region_name="us-west-2",  # change as per your region
    config=Config(connect_timeout=10, read_timeout=120)
)

AGENT_ID = "QDTSICEWAF"          # Your agent id here
AGENT_ALIAS_ID = "SL9BLKZ34G"   # Your agent alias id here

HARD_CODED_TEMPLATE = "A1"
AGENT_PROMPT = (
    "Map the input headers to the correct output headers using the template {} mapping. ".format(HARD_CODED_TEMPLATE) +
    "Return a JSON array, each entry with 'inputheader' and 'mappedheader'. "
    "Include all output headers from the Training Data KB, even if unmapped. "
    "Keep the order exactly as per your KB. "
    "Do not omit any headers for any reason. "
    "Respond only with the JSON array and nothing else."
)

def load_file_once(file_bytes, file_extension):
    """Read file fully once and return DataFrame + headers"""
    try:
        if file_extension == '.csv':
            df = pd.read_csv(BytesIO(file_bytes))
        elif file_extension in ['.xls', '.xlsx']:
            df = pd.read_excel(BytesIO(file_bytes))
        else:
            raise ValueError(f"Unsupported file type: {file_extension}")

        headers = list(df.columns)
        logger.info(f"Extracted headers: {headers}")
        logger.info(f"Input data loaded into DataFrame with shape {df.shape}")
        return df, headers
    except Exception as e:
        logger.error(f"Error reading file: {e}")
        raise


def invoke_agent(headers_csv, template_name):
    """Call the Bedrock agent with prompt and inputs."""
    session_id = f"mapping-{template_name}"
    prompt = AGENT_PROMPT
    payload = f"Template: {template_name}\nInput headers: {headers_csv}\n\n{prompt}"

    logger.info(f"Invoking agent with payload:\n{payload}")

    params = {
        "agentId": AGENT_ID,
        "agentAliasId": AGENT_ALIAS_ID,
        "sessionId": session_id,
        "inputText": payload
    }

    response = bedrock_agent_runtime.invoke_agent(**params)
    full_output = ""
    for event in response.get("completion", []):
        if "chunk" in event and "bytes" in event["chunk"]:
            full_output += event["chunk"]["bytes"].decode("utf-8")

    logger.info(f"Received raw agent response: {full_output}")

    try:
        mappings = json.loads(full_output)
        logger.info(f"Parsed agent mappings JSON with {len(mappings)} entries")
        return mappings
    except Exception as e:
        logger.error(f"Error parsing agent output: {e}")
        logger.error(f"Raw output: {full_output}")
        raise




def create_output_excel(mappings, input_data_df):
    """
    Create a new Excel in memory with mapped headers using pandas (faster).
    """
    mapped_headers = [m['mappedheader'] for m in mappings]
    input_headers_map = {m['mappedheader']: m['inputheader'] for m in mappings}

    logger.info(f"Creating output Excel with columns: {mapped_headers}")

    # Build DataFrame with mapped headers
    df_out = pd.DataFrame()
    for mapped_header in mapped_headers:
        input_header = input_headers_map.get(mapped_header)
        if input_header in input_data_df.columns:
            df_out[mapped_header] = input_data_df[input_header]
        else:
            df_out[mapped_header] = ""  # blank column if not found

    # Save to Excel in memory
    output_stream = BytesIO()
    df_out.to_excel(output_stream, index=False)
    output_stream.seek(0)

    logger.info("Output Excel file created in memory (pandas fast mode)")
    return output_stream


def lambda_handler(event, context):
    try:
        record = event['Records'][0]
        bucket = record['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(record['s3']['object']['key'])
        logger.info(f"Triggered by file: {key} in bucket: {bucket}")

        # Check if in input folder
        if not key.startswith("input/"):
            logger.info("File not in input folder, exiting.")
            return {'statusCode': 200, 'body': 'Not an input file, skipping.'}

        # Check extension
        _, ext = os.path.splitext(key)
        ext = ext.lower()
        supported_exts = ['.csv', '.xls', '.xlsx']
        if ext not in supported_exts:
            logger.error(f"Unsupported file type: {ext}")
            return {'statusCode': 400, 'body': f"Unsupported file type {ext}"}

        logger.info(f"Downloading file {key} from bucket {bucket}")
        s3_obj = s3.get_object(Bucket=bucket, Key=key)
        file_bytes = s3_obj['Body'].read()

        # Extract headers
        # Read file once → get DataFrame + headers
        input_data_df, headers = load_file_once(file_bytes, ext)
        if not headers:
            logger.error("Could not extract headers from file.")
            return {'statusCode': 400, 'body': 'Could not extract headers'}

        headers_csv = ", ".join(headers)

        # Hardcoded template
        template_name = HARD_CODED_TEMPLATE

        # Invoke agent
        mappings = invoke_agent(headers_csv, template_name)

        logger.info(f"Agent mappings: {mappings}")

        # Read full input data into DataFrame
        # Create output excel as BytesIO stream
        output_stream = create_output_excel(mappings, input_data_df)

        # Upload output file to S3 output folder
        output_key = key.replace("input/", "output/").rsplit('.', 1)[0] + ".xlsx"  # Save as xlsx always
        logger.info(f"Uploading output file to {output_key}")
        s3.put_object(Bucket=bucket, Key=output_key, Body=output_stream.getvalue())

        logger.info("Processing complete")
        return {'statusCode': 200, 'body': f"Processed {key}, output saved to {output_key}"}

    except Exception as e:
        logger.error(f"Error processing file: {e}", exc_info=True)
        return {'statusCode': 500, 'body': str(e)}
