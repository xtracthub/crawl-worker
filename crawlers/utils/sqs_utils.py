

import os
import json

import boto3


def _get_dev_status():
    raw_is_dev = os.environ["XTRACT_IS_DEV"]
    print(f"RAW IS DEV: {raw_is_dev}")
    if raw_is_dev == "TRUE":
        is_dev = True
    elif raw_is_dev == "FALSE":
        is_dev = False
    else:
        raise ValueError("COULD NOT FIND XTRACT_IS_DEV ENVIRONMENT VARIABLE ON DISK")
    return is_dev


def get_sqs_conn():

    client = boto3.client('sqs',
                          aws_access_key_id=os.environ["AWS_ACCESS"],
                          aws_secret_access_key=os.environ["AWS_SECRET"], region_name='us-east-1')
    return client


def delete_message(client, msg_info):

    crawl_queue = get_crawl_work_queue()

    client.delete_message_batch(
        QueueUrl=crawl_queue,
        Entries=[msg_info])


def get_crawl_work_queue():

    is_dev = _get_dev_status()

    if is_dev:
        crawl_work_queue = 'https://sqs.us-east-1.amazonaws.com/576668000072/crawl_work_queue_DEBUG'
    else:
        crawl_work_queue = 'https://sqs.us-east-1.amazonaws.com/576668000072/crawl_work_queue'

    return crawl_work_queue


def get_next_task(max_timeout=20, delete_messages=True):

    client = get_sqs_conn()
    crawl_work_queue = get_crawl_work_queue()

    print(crawl_work_queue)

    sqs_response = client.receive_message(  # TODO: properly try/except this block.
        QueueUrl=crawl_work_queue,
        MaxNumberOfMessages=1,  # only 1 because we're not scaling to 'crawl jobs per second'.
        WaitTimeSeconds=max_timeout)

    print("Successfully received task from SQS!")

    message = sqs_response["Messages"][0]["Body"]

    receipt_handle = sqs_response["Messages"][0]["ReceiptHandle"]
    receipt_id = sqs_response["Messages"][0]["MessageId"]

    if delete_messages:
        delete_message(client, {'ReceiptHandle': receipt_handle, 'Id': receipt_id})

    return json.loads(message)


def push_crawl_task(task, unique_id):

    # TODO: when we turn this into a class 'id' should just be a 'count up' variable

    entry = {
        'Id': unique_id,
        'MessageBody': task,

    }

    client = get_sqs_conn()
    client.send_message_batch(QueueUrl=get_crawl_work_queue(),
                              Entries=[entry])

    print("Successfully sent task to SQS!")
