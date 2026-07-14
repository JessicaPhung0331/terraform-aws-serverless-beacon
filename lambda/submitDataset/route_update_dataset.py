import json
import os
import re
import uuid
from threading import Thread

import boto3
from shared.apiutils import build_bad_request, bundle_response
from shared.athena import Snp, Sample, Genotype
from shared.dynamodb import Dataset as DynamoDataset
from smart_open import open as sopen


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
            completed.append("Updated SNP ORC file")
        elif route_type == "genotype":
            threads.append(Thread(target=Genotype.upload_array, args=(parsed_dict,)))
            threads[-1].start()
            completed.append("Updated genotype ORC file")
        if route_type == "sample":
            threads.append(Thread(target=Sample.upload_array, args=(parsed_dict,)))
            threads[-1].start()
            completed.append("Updated samples ORC file")


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


def extract(raw_input, dataset_id, delimiter="", comment=""):
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
            entry = {}
            entry["dataset_id"] = dataset_id
            entry.update(dict(zip(keys, cols)))
            extracted.append(entry)

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


def parse_file(s3_bucket, s3_key, dataset_id):
    print(f"Received bucket {s3_bucket}, key {s3_key}")
   
    if not check_file_exists(s3_bucket, s3_key):
        return bundle_response(
            400, {"message": f"File does not exist on S3. Check your bucket and/or key. Key: {s3_key}"}
        )
   
    # Not sure if utf-8 is always suitable
    with sopen(f"s3://{s3_bucket}/{s3_key}", "rb") as f:
        print(f"Opening file: {s3_key}")
        if (s3_key.endswith(".txt")):
            extracted = extract(f.read().decode("utf-8"), dataset_id, delimiter="\t", comment="#")

        elif (s3_key.endswith(".csv")):
            extracted = extract(f.read().decode("utf-8"), dataset_id, delimiter=",")

        else:
            return bundle_response(
                400, {"message": f"Unsupported file type. Only .txt or .csv files are supported. Ensure your file has the correct suffix and format. File: {s3_key}"}
            )
       
        if not extracted:
            return bundle_response(
                400, {"message": f"Error while parsing file. Check that columns are consistent. File: {s3_key}"}
            )
   
    return extracted


def route(event, dataset_id):
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
        
    # Only read the files that are given
    if snp_key := body_dict.get("snp_key"):
        extracted_outputs.append({"result": parse_file(s3_bucket, snp_key, dataset_id), "type": "snp"})

    if sample_key := body_dict.get("sample_key"):
        extracted_outputs.append({"result": parse_file(s3_bucket, sample_key, dataset_id), "type": "sample"})

    if genotype_keys := body_dict.get("genotype_keys"):
        for genotype_key in genotype_keys:
            extracted_outputs.append({"result": parse_file(s3_bucket, genotype_key, dataset_id), "type": "genotype"})

    errors = []
    correct_outputs = []

    for output in extracted_outputs:
        if (isinstance(output["result"], dict)):
            errors.append(output["result"])
        else:
            correct_outputs.append(output)
   
    submit_dataset(correct_outputs)
   
    return bundle_response(200, {"message": "Successfully scheduled data for submission", "errors": errors, "pending": pending, "completed": completed})


if __name__ == "__main__":
    pass