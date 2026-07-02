import json
import os
from threading import Thread

import boto3
import jsons
from jsonschema import Draft202012Validator, RefResolver
from shared.apiutils import build_bad_request, bundle_response
from shared.athena import Analysis, Biosample, Cohort, Dataset, Individual, Run
from shared.dynamodb import Dataset as DynamoDataset
from shared.utils import clear_tmp
from smart_open import open as sopen
from util import get_vcf_chromosome_maps

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


def create_dataset(attributes, vcf_chromosome_maps):
    datasetId = attributes.get("datasetId", None)
    cohortId = attributes.get("cohortId", None)
    index = attributes.get("index", False)
    global pending, completed
    threads = []

    if datasetId:
        json_dataset = attributes.get("dataset", None)
        if json_dataset:
            # dataset information
            item = DynamoDataset(datasetId)
            item.assemblyId = attributes.get("assemblyId", "UNKNOWN")
            item.vcfLocations = attributes.get("vcfLocations", [])
            item.vcfGroups = attributes.get("vcfGroups", [item.vcfLocations])
            item.vcfChromosomeMap = vcf_chromosome_maps
            item.save()
            completed.append("Added dataset info")

            # dataset metadata entry information
            json_dataset["id"] = datasetId
            json_dataset["assemblyId"] = item.assemblyId
            json_dataset["vcfLocations"] = item.vcfLocations
            json_dataset["vcfChromosomeMap"] = [
                vcfm.attribute_values for vcfm in vcf_chromosome_maps
            ]
            json_dataset["createDateTime"] = str(item.createDateTime)
            json_dataset["updateDateTime"] = str(item.updateDateTime)
            threads.append(Thread(target=Dataset.upload_array, args=([json_dataset],)))
            threads[-1].start()
            completed.append("Added dataset metadata")

    if datasetId and cohortId:
        print("De-serialising started")
        individuals = attributes.get("individuals", [])
        biosamples = attributes.get("biosamples", [])
        runs = attributes.get("runs", [])
        analyses = attributes.get("analyses", [])
        print("De-serialising complete")

        # setting dataset id
        # for example _vcfSampleId is mapped to vcfSampleId
        # skip _ in private variables
        # they are handled in the upload function
        for individual in individuals:
            individual["datasetId"] = datasetId
            individual["cohortId"] = cohortId

        for biosample in biosamples:
            biosample["datasetId"] = datasetId
            biosample["cohortId"] = cohortId

        for run in runs:
            run["datasetId"] = datasetId
            run["cohortId"] = cohortId

        for analysis in analyses:
            analysis["datasetId"] = datasetId
            analysis["cohortId"] = cohortId

        # upload to s3
        if len(individuals) > 0:
            threads.append(Thread(target=Individual.upload_array, args=(individuals,)))
            threads[-1].start()
            completed.append("Added individuals")

        if len(biosamples) > 0:
            threads.append(Thread(target=Biosample.upload_array, args=(biosamples,)))
            threads[-1].start()
            completed.append("Added biosamples")

        if len(runs) > 0:
            threads.append(Thread(target=Run.upload_array, args=(runs,)))
            threads[-1].start()
            completed.append("Added runs")

        if len(analyses) > 0:
            threads.append(Thread(target=Analysis.upload_array, args=(analyses,)))
            threads[-1].start()
            completed.append("Added analyses")

    if cohortId:
        # cohort information
        json_cohort = attributes.get("cohort", None)
        if json_cohort:
            json_cohort["cohortSize"] = len(attributes.get("individuals", []))
            json_cohort["id"] = cohortId
            # Cohort.upload_array([cohort])
            threads.append(Thread(target=Cohort.upload_array, args=([json_cohort],)))
            threads[-1].start()
            completed.append("Added cohorts")

    print("Awaiting uploads")
    [thread.join() for thread in threads]
    print("Upload finished")

    if index:
        aws_lambda.invoke(
            FunctionName=INDEXER_LAMBDA,
            InvocationType="Event",
            Payload=jsons.dumps(dict()),
        )
        pending.append("Running indexer")


def submit_dataset(body_dict):
    global pending, completed
    pending = []
    completed = []

    return bundle_response(
        200,
        {
            "message": "Received dataset submission request"
        },
    )


def validate_columns(raw_content):
    return True


def parse(raw_input, delimiter="", comment=""):
    lines = raw_input.split('\n')
    keys = []
    extracted = []

    for line in lines:
        if line.startswith(comment):
            continue

        cols = line.split(delimiter)

        if len(keys) == 0:
            keys = cols
        else:
            extracted.append(dict(zip(keys, cols)))

    return [keys, extracted]


def parse_file(s3_bucket, s3_key):
    with sopen(f"s3://{s3_bucket}/{s3_key}", "rb") as f:
        if (s3_key.endswith(".txt")):
            extracted = parse(f.read().decode("utf-8"), delimiter="\t", comment="#")

        elif (s3_key.endswith(".csv")):
            extracted = parse(f.read().decode("utf-8"), delimiter=",")

        else:
            print("Unsupported file type. Only .txt or .csv files are supported. Ensure your file has the correct format.")
            return
        
    print(extracted)

    return extracted


def check_file_exists(s3_bucket, s3_key):
    try:
        s3.head_object(Bucket=s3_bucket, Key=s3_key)
        print("File found successfully")

        return True
    except s3.exceptions.ClientError as e:
        if e.response["Error"]["Code"] in {"404", "NoSuchKey"}:
            return False


def route(event):
    # reset progress vars
    global completed, pending

    completed = []
    pending = []

    event_body = event.get("body")

    if not event_body:
        return bundle_response(400, {"message": "No body sent with request."})
    try:
        body_dict = json.loads(event_body)

        if s3_bucket := body_dict.get("s3_bucket"):
            print("Received S3 bucket")

        if s3_key := body_dict.get("s3_key"):
            print("Received S3 key")
    except ValueError:
        return bundle_response(
            400, {"message": "Error parsing request body, Expected JSON."}
        )
    
    # Validate file exists on S3
    if not check_file_exists(s3_bucket, s3_key):
        return bundle_response(
            400, {"message": "File does not exist on S3. Check your bucket and/or key."}
        )
    
    # Validate File Structure (Correct Columns)
    # call func

    if not (extracted_file := parse_file(s3_bucket, s3_key)):
        return bundle_response(
            400, {"message": "Error while parsing file. Check that columns are consistent."}
        )

    # TODO Determine which file is being submitted

    result = submit_dataset(extracted_file)
    clear_tmp()
    return result


if __name__ == "__main__":
    pass