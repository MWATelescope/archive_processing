import boto3
from botocore.client import Config


def ceph_get_s3_object(profile: str, endpoint: str):
    # create a session based on the profile name
    session = boto3.Session(profile_name=profile)

    # This ensures the default boto retries and timeouts don't leave us
    # hanging too long
    config = Config(connect_timeout=20, retries={"max_attempts": 2})

    s3_object = session.resource("s3", endpoint_url=endpoint, config=config)

    return s3_object


def ceph_delete_files(session, bucket: str, keys: []):
    #
    # Delete multiple files (up to 1000)
    #
    # The JSON for the delete is in the form of:
    #
    # [
    #  {'Key': 'filename1'},
    #  {'Key': 'filename2'}
    # ]
    forDeletion = [
        {"Key": "IMG_20160807_150118.jpg"},
        {"Key": "IMG_20160807_150124.jpg"},
    ]
    response = bucket.delete_objects(Delete={"Objects": forDeletion})

    for elem in response.get("Deleted"):
        print(elem["Key"])
