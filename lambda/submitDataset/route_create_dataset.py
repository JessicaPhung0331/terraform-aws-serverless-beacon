import json
import os
import re
from threading import Thread


import boto3
import jsons
from jsonschema import Draft202012Validator, RefResolver
from shared.apiutils import build_bad_request, bundle_response
from shared.athena import Snp, Sample, Genotype
from shared.dynamodb import Dataset as DynamoDataset
from shared.utils import clear_tmp
from smart_open import open as sopen
from util import get_vcf_chromosome_maps, validate_file


DATASETS_TABLE_NAME = os.environ["DYNAMO_DATASETS_TABLE"]
INDEXER_LAMBDA = os.environ["INDEXER_LAMBDA"]


# uncomment below for debugging
# os.environ['LD_DEBUG'] = 'all'
s3 = boto3.client("s3")
sns = boto3.client("sns")
aws_lambda = boto3.client("lambda")

# progress vars
completed = []
pending = []


def submit_dataset(datasets):
    global pending, completed
    threads = []

    for dataset in datasets:
        parsed_dict = dataset["result"]
        route_type = dataset["type"]

        if route_type == "snp":
            threads.append(Thread(target=Snp.upload_array, args=(parsed_dict,)))
            threads[-1].start()
            completed.append("Added SNP data to ORC")
        elif route_type == "genotype":
            threads.append(Thread(target=Genotype.upload_array, args=(parsed_dict,)))
            threads[-1].start()
            completed.append("Added genotype data to ORC")
        if route_type == "sample":
            threads.append(Thread(target=Sample.upload_array, args=(parsed_dict,)))
            threads[-1].start()
            completed.append("Added sample data to ORC")


    print("Awaiting uploads")
    [thread.join() for thread in threads]
    print("Upload finished")


    # if index:
    #     aws_lambda.invoke(
    #         FunctionName=INDEXER_LAMBDA,
    #         InvocationType="Event",
    #         Payload=jsons.dumps(dict()),
    #     )
    #     pending.append("Running indexer")


def extract(raw_input, delimiter="", comment=""):
    unix_text = re.sub(r'\r', '', raw_input)
    lines = unix_text.split('\n')
    keys = []
    extracted = []

    for line in lines:
        if not line or (comment and line.startswith(comment)):
            continue

        cols = line.split(delimiter)

        # Fix column names
        if len(keys) == 0:
            keys = [col.lower().strip().replace(" ", "_") for col in cols]
            print(f"Found columns: {keys}")

        elif len(keys) == len(cols):
            extracted.append(dict(zip(keys, cols)))

        else:
            print("Warning: line may be incorrect")
            print(line)
        
    print("Successfully read file into dict")

    return extracted

def check_file_exists(s3_bucket, s3_key):
    try:
        s3.head_object(Bucket=s3_bucket, Key=s3_key)
        print("File found successfully")
        return True
    except s3.exceptions.ClientError as e:
        if e.response["Error"]["Code"] in {"404", "NoSuchKey"}:
            return False


def parse_file(s3_bucket, s3_key, route_type):
    print(f"Received bucket {s3_bucket}, key {s3_key}, type {route_type}")
   
    if not check_file_exists(s3_bucket, s3_key):
        return bundle_response(
            400, {"message": f"File does not exist on S3. Check your bucket and/or key. Key: {s3_key}"}
        )
   
    # Not sure if utf-8 is always suitable
    with sopen(f"s3://{s3_bucket}/{s3_key}", "rb") as f:
        print(f"Opening file: {s3_key}")
        if (s3_key.endswith(".txt")):
            extracted = extract(f.read().decode("utf-8"), delimiter="\t", comment="#")

        elif (s3_key.endswith(".csv")):
            extracted = extract(f.read().decode("utf-8"), delimiter=",")

        else:
            return bundle_response(
                400, {"message": f"Unsupported file type. Only .txt or .csv files are supported. Ensure your file has the correct suffix and format. File: {s3_key}"}
            )
       
        if not extracted:
            return bundle_response(
                400, {"message": f"Error while parsing file. Check that columns are consistent. File: {s3_key}"}
            )
        
        validation_error = validate_file(route_type, extracted)

        if validation_error:
            return bundle_response(
                400, {"message": f"Error while validating {s3_key}. {validation_error}"}
            )
   
    return extracted


def route(event, route_type=""):
    # reset progress vars
    global completed, pending

    completed = []
    pending = []
    extracted_outputs = []

    event_body = event.get("body")

    if not event_body:
        return bundle_response(400, {"message": "No body sent with request."})

    try:
        body_dict = json.loads(event_body)
    except ValueError:
        return bundle_response(
            400, {"message": "Error parsing request body, Expected JSON."}
        )
    
    s3_bucket = body_dict.get("s3_bucket")

    if not s3_bucket:
        return bundle_response(400, {"message": "No bucket specified."})

    if route_type:
        route_type = route_type.lower().strip()
        print(route_type)

        if route_type not in ["snp", "sample", "genotype"]:
            return bundle_response(400, {"message": "Invalid parameter. Expected 'snp', 'sample', or 'genotype'."})
        
        if s3_key := body_dict.get(f"{route_type}_key"):
            extracted_outputs.append({"result": parse_file(s3_bucket, s3_key, route_type), "type": route_type})
        
    else:
        
        # Support mutliple submissions
        if snp_key := body_dict.get("snp_key"):
            extracted_outputs.append({"result": parse_file(s3_bucket, snp_key, "snp"), "type": "snp"})

        if sample_key := body_dict.get("sample_key"):
            extracted_outputs.append({"result": parse_file(s3_bucket, sample_key, "sample"), "type": "sample"})

        if genotype_key := body_dict.get("genotype_key"):
            extracted_outputs.append({"result": parse_file(s3_bucket, genotype_key, "genotype"), "type": "genotype"})

    for output in extracted_outputs:
        if (isinstance(output["result"], dict) and output["result"].get("statusCode") == 400):
            return output["result"]
   
    submit_dataset(extracted_outputs)
   
    return bundle_response(200, {"message": "Successfully submitted all data.", "pending": pending, "completed": completed})


if __name__ == "__main__":
    pass