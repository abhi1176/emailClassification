
import argparse
import boto3
import json
import os
import tarfile
import time


class EmailClassifier(object):

    REGION = "ap-southeast-2"
    DOCUMENT_CLASSIFIER_ARN = "arn:aws:comprehend:ap-southeast-2:896473863862:document-classifier/EmailClassifier-copy"
    DATA_ACCESS_ROLE_ARN = "arn:aws:iam::896473863862:role/service-role/AmazonComprehendServiceRole-Analysis"
    JOB_NAME = "EmailAnalysis_test"
    INPUT_BUCKET = "twentynews2"
    INPUT_FILENAME = "test_file.txt"
    OUTPUT_BUCKET = "twentynews.output"
    INPUT_FORMAT = "ONE_DOC_PER_FILE"

    def __init__(self, message=None, file=None):
        if args.message:
            with open("message_to_s3.txt", "w") as f:
                f.write(args.message.decode('ascii', 'ignore').encode('ascii'))
        else:
            self.encode_file_to_utf8(file)
        self.upload_text_to_s3("message_to_s3.txt")
        os.remove("message_to_s3.txt")

    def encode_file_to_utf8(self, file_name):
        sourceEncoding = "iso-8859-1"
        targetEncoding = "utf-8"
        with open(file_name) as source:
            with open("message_to_s3.txt", "w") as target:
                target.write(unicode(source.read(),
                                     sourceEncoding).encode(targetEncoding))

    def upload_text_to_s3(self, file):
        s3 = boto3.resource('s3')
        s3.Bucket(self.INPUT_BUCKET).upload_file(file, self.INPUT_FILENAME)
        self.input_s3_uri = "s3://{}/{}".format(self.INPUT_BUCKET, self.INPUT_FILENAME)

    def predict_class(self):
        self.client = boto3.client('comprehend', region_name=self.REGION)
        start_response = self.client.start_document_classification_job (
            InputDataConfig={
                'S3Uri': self.input_s3_uri,
                'InputFormat': self.INPUT_FORMAT
            },
            OutputDataConfig={
                'S3Uri': "s3://{}".format(self.OUTPUT_BUCKET)
            },
            JobName=self.JOB_NAME,
            DataAccessRoleArn=self.DATA_ACCESS_ROLE_ARN,
            DocumentClassifierArn=self.DOCUMENT_CLASSIFIER_ARN
        )
        describe_response = self.client.describe_document_classification_job(JobId=start_response['JobId'])
        self.output_s3_uri = describe_response['DocumentClassificationJobProperties']['OutputDataConfig']['S3Uri']
        self.job_id = describe_response['DocumentClassificationJobProperties']['JobId']

    def get_predictions(self):
        job_status = self.client.describe_document_classification_job(
            JobId=self.job_id)['DocumentClassificationJobProperties']['JobStatus']

        while job_status in ["IN_PROGRESS", "SUBMITTED"]:
            time.sleep(30)
            job_status = self.client.describe_document_classification_job(
                JobId=self.job_id)['DocumentClassificationJobProperties']['JobStatus']

        s3_client = boto3.client('s3')
        key = self.output_s3_uri.split(self.OUTPUT_BUCKET)[1].strip("/")
        try:
            s3_client.download_file(
                Bucket=self.OUTPUT_BUCKET,
                Key=key,
                Filename="output.tar.gz")
        except Exception as e:
            print e.message
        else:
            with tarfile.open("output.tar.gz", "r:gz") as tar:
                tar.extractall()
            with open("predictions.jsonl") as f:
                data = json.load(f)
            for cls in data['Classes']:
                print "{} / Probability: {}".format(cls['Name'], cls['Score'])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--message", help="Email body message")
    group.add_argument("--file", help="File containing email message content")
    args = parser.parse_args()
    e = EmailClassifier(args.message, args.file)
    e.predict_class()
    print "Output S3 Uri: {}".format(e.output_s3_uri)
    print "Job id: {}".format(e.job_id)
    e.get_predictions()
