import json
import logging
import os
from collections import defaultdict
from datetime import datetime
from time import sleep
import urllib.request
import urllib.error
from urllib.parse import quote, urlencode
import fitz  # PyMuPDF
from io import BytesIO
import base64

import boto3
import pymssql  # For database connection

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3_client = boto3.client('s3')
textract_client = boto3.client('textract')

# Initialize database connection details
DB_HOST = 'ec2-34-220-22-254.us-west-2.compute.amazonaws.com'
DB_USERNAME = 'devuser'
DB_PASSWORD = 'Test@123'
DB_NAME = 'pdfExtraction'

# Environment variables
WAIT_TIME_FOR_RESULTS = int(os.environ.get("WAIT_TIME_FOR_RESULTS", 2))
INBOUND_FOLDER = 'inbound/'
OUTBOUND_FOLDER = 'outbound/'
REVIEW_FOLDER = 'review/'
OUTPUT_RESPONSE_FOLDER = 'output_response'
MESSAGE_PROCESSED_BY = "system"
PROCESSED_MESSAGE_STATUS = 'Processed'
PARTIALLY_PROCESSED_MESSAGE_STATUS = 'Partially Processed'


def call_http_api(url, headers=None, json_data=None):
    logger.info('call_http_api started')
    try:
        # Check if we're sending JSON data (POST request)
        if json_data:
            # Convert the JSON data to bytes
            data = json.dumps(json_data).encode('utf-8')
            logger.info(f'data: {data}')
            logger.info('Calling POST API')
            # Create a POST request
            request = urllib.request.Request(url, data=data, headers=headers or {}, method='POST')
        else:
            logger.info('Calling GET API')
            # Create a GET request
            request = urllib.request.Request(url, headers=headers or {}, method='GET')

        # Send the request
        with urllib.request.urlopen(request) as response:
            # Read and decode the response
            response_data = response.read().decode('utf-8')

            # Try to parse the response as JSON
            try:
                logger.info('call_http_api ended')
                parsed_data = json.loads(response_data)
                logger.info(f'parsed_data: {parsed_data}')

                return parsed_data
            except json.JSONDecodeError:
                # If the response is not JSON, return it as text
                return response_data

    except urllib.error.HTTPError as http_err:
        logger.info(f"HTTP error occurred: {http_err.code} - {http_err.reason}")
        return None
    except urllib.error.URLError as url_err:
        logger.info(f"URL error occurred: {url_err.reason}")
        return None
    except Exception as err:
        logger.error(f"An error occurred: {err}")
        return None


def retrieve_file_meta_data_using_api(bucket_name, object_key, file_name, prompt):
    # Encode the object key to handle spaces and special characters
    encoded_file_path = quote(f"{object_key}")
    # Define the API endpoint
    api_url = " https://gmls40yk6c.execute-api.us-west-2.amazonaws.com/PROD/extractFileMetaDataWithPrompt"
    logger.info(f'Calling API: {api_url} path: {encoded_file_path}, bucket: {bucket_name}, prompt : {prompt}')

    # Create the JSON data for the POST request
    json_data = {
        "bucketName": bucket_name,
        "filePath": object_key,
        "prompt": prompt
    }

    # Serialize the dictionary to JSON string
    # json_data = json.dumps(request_body_data).encode('utf-8')

    logger.info(f'json_data: {json_data}')
    headers = {
        'Content-Type': 'application/json'  # Ensure headers specify JSON content if needed by API
    }

    # Call API with POST request
    response_data = call_http_api(api_url, headers=headers, json_data=json_data)
    logger.info(f'API response_data: {response_data}')

    # Determine which JSON config file to use based on the file name
    # config_mapping = {
    #     'combinedPdfFileForUI_sample': 'config_files/combinedPdfFileForUI_sample.json',
    #     'customer1-attachment1': 'config_files/customer1-attachment1.json',
    #     'HDFC_FORM_Multi_Product': 'config_files/HDFC_FORM_Multi_Product.json',
    #     'SBI_Account_Closure_form': 'config_files/SBI_Account_Closure_form.json'
    # }
    # json_file = next((file for key, file in config_mapping.items() if key in file_name), None)

    # Load metadata if a JSON config file was found
    meta_data = None
    if response_data:
        meta_data = response_data
    else:
        logger.error(f"No matching config file found for: {file_name}")

    return meta_data


def process_file_meta_data(meta_data_dict):
    # Initialize defaultdict to store the key-value pairs by page number
    page_wise_kvs = defaultdict(list)

    # Extracting text items
    text_items = meta_data_dict["body"]["content"][0]["text"]
    # Iterate over key-value pairs in each metadata item
    for key, value in text_items.items():

        if value != '':
            page_wise_kvs[0].append({
                'key': key,
                'value': value,
                'key_confidence': 0.0,
                'value_confidence': 0.0,
                'page_number': 0,
                "display_order": 1
            })

    # Sort the key-value pairs by display_order for each page
    # for page_number in page_wise_kvs:
    # page_wise_kvs[page_number].sort(key=lambda x: x['display_order'])

    return page_wise_kvs


def load_json_file(json_file):
    """Load JSON data from a file."""
    try:
        with open(json_file, 'r') as file:
            return json.load(file)
    except (json.JSONDecodeError, FileNotFoundError) as e:
        print(f"Error loading JSON: {e}")
        return None
def convert_pdf_to_png(pdf_data):
    """Convert each page of the PDF to PNG format and include page numbers."""
    logger.info("Starting convert_pdf_to_png method")
    pdf_document = fitz.open(stream=pdf_data, filetype="pdf")
    png_images = []

    for page_num in range(len(pdf_document)):
        page = pdf_document.load_page(page_num)
        pix = page.get_pixmap()  # Convert page to image
        png_image_data = pix.tobytes("png")  # Get image data as PNG in bytes
        png_images.append({"page_number": page_num + 1, "image_data": png_image_data})  # Store page number and image
        logger.info(f"Converted PDF Page number {page_num + 1} to PNG")

    logger.info("Finished convert_pdf_to_png method")
    return png_images

def create_combined_prompt(png_images, received_prompt):
    """Create a single prompt for AWS Bedrock with all images in the multi-page PDF."""
    logger.info("Starting create_combined_prompt method")

    # Use a default prompt if none provided
    default_prompt = """
    This PDF contains a bank closure form. 
    Please precisely copy all the relevant information from the form across all pages.
    Leave the field blank if there is no information in the corresponding field.
    If the form does not contain bank closure information, simply return an empty JSON object. 
    Translate any non-English text to English. 
    Organize and return the extracted data in a JSON format with the following keys:
    {
      "ACCOUNT_HOLDER_NAME": "", "MOBILE_NUMBER": "", "ACCOUNT_NUMBER": "", 
      "TRANSFER_ACCOUNT_NUMBER": "", "SAVINGS_ACCOUNT_NUMBER": "", "EMAIL_ADDRESS": "", 
      "PAY_ORDER_OR_DD": "", "BRANCH_CODE": "", "RECEIVERS_NAME": "", "CITY": "", 
      "PIN_CODE": "", "FIRST_APPLICANT": "", "DATE": "", "BANKERS_CHEQUE_OR_DRAFT": "", 
      "NAME_OF_BANK": "", "DISTRICT": "", "BRANCH": "", "STATE": "", "COUNTRY": "", 
      "RECEIVER_NAME": "", "ADDRESS": "", "CREDIT_CARD_NUMBER": "", 
      "REASON_FOR_CARD_CLOSURE": ""
    }
    """

    # Use received prompt if provided, otherwise use default
    combined_prompt_text = received_prompt if received_prompt else default_prompt
    combined_images = []

    # Convert each image to base64 and add to the combined list
    for img in png_images:
        img_base64 = base64.b64encode(img["image_data"]).decode('utf-8')
        combined_images.append({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": "image/png",
                "data": img_base64
            }
        })

    # Construct the prompt with all pages combined
    prompt = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 2048,
        "messages": [
            {
                "role": "user",
                "content": combined_images + [{"type": "text", "text": combined_prompt_text}]
            }
        ]
    }

    logger.info(f"prompt: {prompt}")

    logger.info("Finished create_combined_prompt method")
    return prompt

def send_combined_prompt_to_bedrock(prompt):
    """Send the combined prompt with all pages to AWS Bedrock and get a single response."""
    logger.info("Starting send_combined_prompt_to_bedrock method")
    bedrock_client = boto3.client('bedrock-runtime', region_name='us-west-2')

    # Invoke the Bedrock model with a single combined request
    response = bedrock_client.invoke_model(
        modelId='anthropic.claude-3-5-sonnet-20240620-v1:0',  # Replace with your model ID
        body=json.dumps(prompt)
    )

    # Parse and return the response
    response_body = response['body'].read().decode('utf-8')

    logger.info(f"Raw response_body : {response_body}")

    # Load the JSON to parse and handle the text field
    response_data = json.loads(response_body)
    # Update the "text" field to clean up escape characters
    response_data["content"][0]["text"] = json.loads(response_data["content"][0]["text"])
    logger.info(f"response_data : {response_data}")

    logger.info("Finished send_combined_prompt_to_bedrock method")
    return response_data

def process_file_with_prompt(pdf_data, prompt):

    png_images = convert_pdf_to_png(pdf_data)
    prompts = create_combined_prompt(png_images, prompt)
    responses = send_combined_prompt_to_bedrock(prompts)
    logger.info("Extracted JSON output: %s", json.dumps(responses, indent=2))
    logger.info("Finished process_file_with_prompt method")

    response_dict = dict()
    response_dict['statusCode'] = 200
    response_dict['body'] = responses
    return {
        "statusCode": 200,
        # "body": json.dumps({"responses": responses})
        "body": responses
    }



def lambda_handler(event, context):
    """
    Entry point for AWS Lambda function. Processes SQS events, fetches attachments from S3,
    extracts data using Textract, and inserts it into the database.
    """

    for record in event['Records']:
        message_body = record['body']
        message_json = json.loads(message_body)

        # Dictionary to store extracted data by process_attachment_id
        attachment_response = defaultdict(list)

        # Extracting mime_type at the process_mail_id level
        content_type = message_json.get("content_type", "").upper()

        # Check if the mime_type is MIME or RTF, otherwise skip processing
        if content_type not in ['MULTIPART/MIXED']:
            logger.info(
                f"Skipping processing for process_mail_id: {message_json.get('process_id')}, invalid content_type: {content_type}")
            continue

        # Iterate over attachments and process them
        attachments = message_json.get("attachments", [])
        process_mail_id = message_json.get("process_id")

        for attachment in attachments:
            process_attachment_id = attachment['process_attachment_id']
            bucket_name = attachment['s3_bucket']
            s3_object_path = attachment['s3_object_path']
            object_key = f"{attachment['s3_object_path']}/{attachment['file_name']}"

            # Download the PDF
            pdf_content = download_pdf_from_s3(bucket_name, object_key)

            if not pdf_content:
                # If download fails, move the file to the review folder
                move_file_in_s3(bucket_name, object_key, REVIEW_FOLDER)
                update_process_attachment(process_attachment_id, s3_object_path, MESSAGE_PROCESSED_BY, REVIEW_FOLDER)
                update_process_data(process_mail_id, PARTIALLY_PROCESSED_MESSAGE_STATUS, MESSAGE_PROCESSED_BY)
                continue

            try:

                # Initialize the variables with default values
                # sorted_page_wise_kvs = None
                table_data = None

                # prompt = """This image shows a bank closure form. \nPlease precisely copy all the relevant information from the form.\nLeave the field blank if there is no information in corresponding field.\nIf the image is not a bank closure form, simply return an empty JSON object. \nOrganize and return the extracted data in a JSON format with the following keys:\n\n  'ACCOUNT_HOLDER_NAME'\n  'MOBILE_NUMBER'\n  'TRANSFER_ACCOUNT_NUMBER'\n  'SAVINGS_ACCOUNT_NUMBER'\n  'EMAIL_ADDRESS'\n  'DATE'\n  'NAME_OF_BANK'\n  'CREDIT_CARD_NUMBER'\n  'REASON_FOR_CARD_CLOSURE'.\nOnly return the extracted data as JSON. Do not include any additional text, explanations, or formatting.
                # """

                new_prompt = (
                                "You are a highly accurate document understanding system.\n\n"
                                "Your task is to extract all key-value pairs from the provided PDF document in a structured JSON format.\n"
                                "The PDF contains multiple sections such as CUSTOMER, COBUYER, DEALER INFORMATION,VEHICLE INFORMATION  etc. Each section may repeat the same keys such as 'First Name', 'Last Name', 'Address', etc.\n\n"
                                "Instructions:\n"
                                "- For each key-value pair, include:\n"
                                "    - section: the section where the key-value appears (e.g., CUSTOMER, COBUYER, etc.)\n"
                                "    - key: the exact key text as it appears in the document\n"
                                "    - value: the exact corresponding value found next to the key\n"
                                "    - confidence: extraction confidence as a percentage from 1.00 to 100.00 \n"
                                "- Do NOT hallucinate or guess values. If a value is missing, unreadable, or ambiguous, set value to \"NOT_FOUND\" and confidence to 0.\n"
                                "- For checkboxes, return value as 'SELECTED' or 'NOT_SELECTED'. If unclear, return 'NOT_SELECTED' and confidence 0.\n"
                                "- Maintain correct mappings — ensure that each value belongs to the correct key and correct section.\n"
                                "- Avoid merging unrelated fields or mislabeling keys/values.\n"
                                "- Do not modify wording. Preserve the original key names and values as they appear in the document.\n\n"
                                "Output format:\n"
                                "[\n"
                                "  {\n"
                                "    \"section\": \"CUSTOMER\",\n"
                                "    \"key\": \"First Name\",\n"
                                "    \"value\": \"John\",\n"
                                "    \"confidence\": 98.12\n"
                                "  },\n"
                                "  {\n"
                                "    \"section\": \"COBUYER\",\n"
                                "    \"key\": \"Address\",\n"
                                "    \"value\": \"123 Main Street, NY\",\n"
                                "    \"confidence\": 95.45\n"
                                "  },\n"
                                "  {\n"
                                "    \"section\": \"GUARANTOR\",\n"
                                "    \"key\": \"Checkbox - Terms Accepted\",\n"
                                "    \"value\": \"SELECTED\",\n"
                                "    \"confidence\": 100.00\n"
                                "  },\n"
                                "  {\n"
                                "    \"section\": \"CUSTOMER\",\n"
                                "    \"key\": \"Middle Name\",\n"
                                "    \"value\": \"NOT_FOUND\",\n"
                                "    \"confidence\": 0.00\n"
                                "  }\n"
                                "]\n\n"
                                "Only return the structured JSON array as shown above — no extra explanation, no markdown, no preamble."
                            )

                # optimized_prompt = (
                #                 "You are a highly accurate document understanding system.\n\n"
                #                 "Your task is to extract all key-value pairs from the provided PDF document into a compact, grouped JSON format.\n"
                #                 "The PDF contains sections such as CUSTOMER, COBUYER, DEALER INFORMATION, VEHICLE INFORMATION, etc. Each section may include repeated keys like 'First Name', 'Address', etc.\n\n"
                #                 "The PDF contains multiple tables with headers like Repair Order No:, 12597, Repair Order Date:, 02/11/2025 and another table consist Line,Quantity, Component, Part No., Unit Price, Total, Description and there are mutiple tables and with headers and column data"
                #                 "The PDF Contains multiple tables with name or without name for example few table names are Parts,Labour. These table contains the Data in it"
                #                 "Instructions:\n"
                #                 "- Group key-value pairs by their respective section name.\n"
                #                 "- Each entry should include:\n"
                #                 "    - k: exact key text as in the document\n"
                #                 "    - v: exact value next to the key\n"
                #                 "    - c: confidence from 1.00 to 100.00\n"
                #                 "- If a value is missing, unreadable, or ambiguous, set v to \"NOT_FOUND\" and c to 0.\n"
                #                 "- For checkboxes, set v to \"SELECTED\" or \"NOT_SELECTED\". If unclear, use \"NOT_SELECTED\" with c as 0.\n"
                #                 "- Preserve all original wording — do not paraphrase or guess.\n"
                #                 "- Return JSON only. No markdown, explanation, or extra formatting.\n\n"
                #                 "Output format:\n"
                #                 "{\n"
                #                 "  \"CUSTOMER\": [\n"
                #                 "    {\"k\": \"First Name\", \"v\": \"John\", \"c\": 98.12},\n"
                #                 "    {\"k\": \"Last Name\", \"v\": \"Doe\", \"c\": 97.55}\n"
                #                 "  ],\n"
                #                 "  \"COBUYER\": [\n"
                #                 "    {\"k\": \"Address\", \"v\": \"123 Main St\", \"c\": 95.12}\n"
                #                 "  ]\n"
                #                 "}"
                #             )


                optimized_prompt = """You are a highly accurate document data extraction system.

                                        Your task: 
                                        1. Carefully read the attached document (scanned PDF pages as images).  
                                        2. Extract the required fields and tables.  
                                        3. Return only a **valid JSON object** in the exact schema described below — no extra text, no explanations.  
                                        4. If a field is not present in the document, return it as null.  
                                        5. For tables:  
                                        - Use the table name (found above the table in the document) as the JSON key.  
                                        - Convert each row into an object.  
                                        - Use the table header names as keys.  
                                        - Wrap all rows inside a list.  

                                        ---

                                        ### JSON Schema to follow:

                                        {
                                        "data": {
                                            "roNumber": string or null,
                                            "roDate": string (MM/DD/YYYY) or null,
                                            "servicer": string or null,
                                            "dateOfLoss": string (MM/DD/YYYY) or null,
                                            "vin": string or null,
                                            "invoiceAmount": string or null,
                                            "totalCharges": string or null,
                                            "taxRate": {
                                            "taxRate": string or null,
                                            "salesTax": string or null,
                                            "taxStates": string or null
                                            },
                                            "mileage": {
                                            "mileageIn": string or null,
                                            "mileageOut": string or null
                                            },
                                            "parts": [
                                            {
                                                "line": string or null,
                                                "quantity": string or null,
                                                "component": string or null,
                                                "partNumber": string or null,
                                                "unitPrice": string or null,
                                                "totalAmount": string or null,
                                                "description": string or null
                                            }
                                            ],
                                            "labour": [
                                            {
                                                "line": string or null,
                                                "type": string or null,
                                                "hours": string or null,
                                                "rate": string or null,
                                                "total": string or null,
                                                "tech": string or null,
                                                "opcode": string or null
                                            }
                                            ]
                                        }
                                        }

                                        ---

                                        ### Important Notes:
                                        - Ensure the output is strictly valid JSON (no trailing commas, no comments).  
                                        - Dates must be in MM/DD/YYYY format if present.  
                                        - Currency and numeric values must be captured as strings exactly as in the document.  
                                        - If multiple tables exist (like Parts, Labour, Taxes, Discounts, etc.), each table should be represented as a list of objects under its respective JSON key (use the table name in lowercase as the key).  
                                        - Do not invent fields not present in the schema.  
                                        - For any missing value, return null.  
                                        """


                # meta_data_dict: dict = retrieve_file_meta_data_using_api(bucket_name, object_key,
                #                                                          attachment['file_name'], new_prompt)
                meta_data_dict: dict = process_file_with_prompt(pdf_content,optimized_prompt)

                if meta_data_dict is not None:

                    update_process_attachment_ai_output(process_attachment_id, meta_data_dict)

                    sorted_page_wise_kvs = process_file_meta_data(meta_data_dict)
                    # Insert extracted key-value pairs into the database

                    # Adding Dummy value for the flow to work end to end
                    # sorted_page_wise_kvs = defaultdict(list)
                    # sorted_page_wise_kvs[1].append({
                    #     'key': 'DUMMY FOR AI MODEL',
                    #     'value': 'DUMMY FOR AI MODEL',
                    #     'key_confidence': 0.0,
                    #     'value_confidence': 0.0,
                    #     'page_number': 1,
                    #     "display_order": 1
                    # })

                    insertion_successful = insert_data_into_db_process_content(process_mail_id,
                                                                               process_attachment_id,
                                                                               sorted_page_wise_kvs)

                    # Move the file based on the result of the insertion
                    if insertion_successful:
                        move_file_in_s3(bucket_name, object_key, OUTBOUND_FOLDER)
                        update_process_attachment(process_attachment_id, s3_object_path, MESSAGE_PROCESSED_BY,
                                                  OUTBOUND_FOLDER)
                        update_process_data(process_mail_id, PROCESSED_MESSAGE_STATUS, MESSAGE_PROCESSED_BY)
                    else:
                        move_file_in_s3(bucket_name, object_key, REVIEW_FOLDER)
                        update_process_attachment(process_attachment_id, s3_object_path, MESSAGE_PROCESSED_BY,
                                                  REVIEW_FOLDER)
                        update_process_data(process_mail_id, PARTIALLY_PROCESSED_MESSAGE_STATUS,
                                            MESSAGE_PROCESSED_BY)

                else:
                    print("No forms data found.")



            except Exception as e:
                # In case Textract or processing fails, log the error and move the file to the review folder
                logger.error(f"Failed to process the file with Textract or other errors: {e}")
                move_file_in_s3(bucket_name, object_key, REVIEW_FOLDER)
                update_process_attachment(process_attachment_id, s3_object_path, MESSAGE_PROCESSED_BY, REVIEW_FOLDER)
                update_process_data(process_mail_id, PARTIALLY_PROCESSED_MESSAGE_STATUS, MESSAGE_PROCESSED_BY)

        # After processing all attachments, modify the original SQS message
        for attachment in message_json['attachments']:
            process_attachment_id = attachment['process_attachment_id']

            # Add extracted contents to the respective attachment in the original SQS message
            attachment['contents'] = attachment_response.get(process_attachment_id, [])

        payload_json = json.dumps(message_json, indent=4)
        # Print the entire modified SQS message
        logging.info(f'processed_json_response: {payload_json}')

        # Generate a unique file name
        filename = create_filename(process_mail_id)
        output_key = f"{OUTPUT_RESPONSE_FOLDER}/{filename}"

        # Write JSON to S3
        # upload_json_to_s3(bucket_name, output_key, message_json)

    return {
        'statusCode': 200,
        'body': 'Processed SQS messages successfully'
    }


def create_filename(process_id):
    # Format the current date and time separately
    date_str = datetime.utcnow().strftime('%Y%m%d')  # Date in YYYYMMDD format
    time_str = datetime.utcnow().strftime('%H%M%S')  # Time in HHMMSS format
    return f"{process_id}_{date_str}_{time_str}.json"


# ======================================================================

def download_pdf_from_s3(bucket_name, object_key):
    """
    Downloads the PDF file from the given S3 bucket and returns the file content.
    """
    try:
        logger.info(f"Downloading PDF from S3: bucket={bucket_name}, key={object_key}")
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        return response['Body'].read()
    except Exception as e:
        logger.error(f"Failed to download PDF from S3: {e}")
        return None


def move_file_in_s3(bucket_name, object_key, target_folder):
    new_object_key = object_key.replace(INBOUND_FOLDER, target_folder, 1)
    try:
        s3_client.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': object_key},
            Key=new_object_key
        )
        # s3_client.delete_object(
        #     Bucket=bucket_name,
        #     Key=object_key
        # )
        logger.info(f"Successfully moved file from {object_key} to {new_object_key}")
    except Exception as e:
        logger.error(f"Failed to move file in S3: {e}")


def upload_json_to_s3(bucket_name, object_key, data):
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=json.dumps(data, indent=4),
            ContentType='application/json'
        )
        logger.info(f"Successfully uploaded response JSON to S3: {object_key}")
    except Exception as e:
        logger.error(f"Failed to upload response JSON to S3: {e}")


# ======================================================================

def update_process_attachment(process_attachment_id, s3_object_path, processed_by, target_folder):
    conn = None
    cursor = None

    modified_object_path = s3_object_path.replace(INBOUND_FOLDER, target_folder, 1)

    try:
        # Establish the connection
        conn = pymssql.connect(DB_HOST, DB_USERNAME, DB_PASSWORD, DB_NAME)
        cursor = conn.cursor()

        # Define the SQL update query
        query = """
        UPDATE process_attachment
        SET
            s3_object_path = %s,
            processed_by = %s,
            processed_date = %s
        WHERE
            process_attachment_id = %s
        """

        # Define the parameters
        params = (modified_object_path, processed_by, datetime.now(), process_attachment_id)

        # Execute the query
        cursor.execute(query, params)

        # Commit the transaction
        conn.commit()

        print(f"Successfully updated process_attachment_id {process_attachment_id}")

    except pymssql.Error as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def update_process_attachment_ai_output(process_attachment_id, ai_output_json):
    conn = None
    cursor = None

    try:
        # Establish the connection
        conn = pymssql.connect(DB_HOST, DB_USERNAME, DB_PASSWORD, DB_NAME)
        cursor = conn.cursor()

        # Define the SQL update query
        query = """
        UPDATE process_attachment
        SET
            ai_output_json = %s
        WHERE
            process_attachment_id = %s
        """

        # Convert ai_output_json to a JSON string
        ai_output_json_str = json.dumps(ai_output_json)

        # Define the parameters
        params = (ai_output_json_str, process_attachment_id)

        # Execute the query
        cursor.execute(query, params)

        # Commit the transaction
        conn.commit()

        print(f"Successfully updated ai_output_json column for process_attachment_id: {process_attachment_id} ")

    except pymssql.Error as e:
        print(f"An error occurred in update_process_attachment_ai_output : {e}")

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def update_process_data(process_id, message_status, message_processed_by):
    conn = None
    cursor = None

    try:
        # Establish the connection
        conn = pymssql.connect(DB_HOST, DB_USERNAME, DB_PASSWORD, DB_NAME)
        cursor = conn.cursor()

        # Define the SQL update query
        query = """
        UPDATE process_data
        SET
            message_status = %s,
            message_processed_by = %s,
            message_processed_date = %s
        WHERE
            process_id = %s
        """

        # Define the parameters
        params = (message_status, message_processed_by, datetime.now(), process_id)

        # Execute the query
        cursor.execute(query, params)

        # Commit the transaction
        conn.commit()

        print(f"Successfully updated process_id {process_id}")

    except pymssql.Error as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def insert_data_into_db_process_content(process_id, process_attachment_id, sorted_page_wise_kvs):
    conn = None
    try:
        conn = pymssql.connect(DB_HOST, DB_USERNAME, DB_PASSWORD, DB_NAME)
        cursor = conn.cursor()

        query = """
            INSERT INTO process_content_ai (
                process_attachment_id, 
                process_id, 
                extract_type, 
                description, 
                page_number, 
                field_key, 
                field_value, 
                confidence_score_key, 
                confidence_score_value, 
                processed_by, 
                processed_date, 
                updated_by, 
                updated_date,
                key_value_sequence
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, GETDATE(), %s, GETDATE(), %s)
        """

        records = []
        processed_by = 'AI_Model'
        updated_by = 'System'

        # Process each page separately
        for page_number, kvs in sorted_page_wise_kvs.items():
            # Add key-value pairs with their order within the page
            for key_value_sequence, entry in enumerate(kvs, start=1):
                records.append(
                    (
                        process_attachment_id,
                        process_id,
                        'form',
                        None,
                        entry['page_number'],  # This will always be the same for entries of the same page
                        entry['key'],
                        entry['value'],
                        entry['key_confidence'],
                        entry['value_confidence'],
                        processed_by,
                        updated_by,
                        key_value_sequence  # Use the order of the key-value pair within the page
                    )
                )

        if records:
            cursor.executemany(query, records)
            conn.commit()
            logger.info(f"Successfully inserted {len(records)} key-value pairs into the database")
            return True
        else:
            logger.info("No key-value pairs to insert")
            return False

    except Exception as e:
        logger.error(f"Failed to insert data into the database: {e}")
        return False

    finally:
        if conn:
            conn.close()