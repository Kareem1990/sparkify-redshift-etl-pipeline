import boto3
import configparser
import json
import time
import pandas as pd
from botocore.exceptions import ClientError

# === Step 0: Load configuration values from dwh.cfg ===
config = configparser.ConfigParser()
config.read('dwh.cfg')

# AWS credentials (including temporary session token)
KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')
SESSION = config.get('AWS', 'SESSION')
REGION = 'us-west-2'  # Must match the region of the S3 bucket

# DWH parameters
DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")
DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_DB = config.get("DWH", "DWH_DB")
DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
DWH_PORT = config.get("DWH", "DWH_PORT")
DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

# === Step 1: Create AWS clients ===
ec2 = boto3.resource('ec2', region_name=REGION,
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     aws_session_token=SESSION)

iam = boto3.client('iam', region_name=REGION,
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET,
                   aws_session_token=SESSION)

redshift = boto3.client('redshift', region_name=REGION,
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET,
                        aws_session_token=SESSION)

# === Step 2: Create IAM Role for Redshift to access S3 ===
try:
    print("Creating IAM Role...")
    iam.create_role(
        Path='/',
        RoleName=DWH_IAM_ROLE_NAME,
        Description="Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument=json.dumps({
            'Statement': [{
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'},
                'Action': 'sts:AssumeRole'
            }],
            'Version': '2012-10-17'
        })
    )
except ClientError as e:
    if e.response['Error']['Code'] == 'EntityAlreadyExists':
        print("IAM Role already exists. Continuing...")
    else:
        raise

# Attach AmazonS3ReadOnlyAccess policy to the IAM Role
print("Attaching AmazonS3ReadOnlyAccess policy...")
iam.attach_role_policy(
    RoleName=DWH_IAM_ROLE_NAME,
    PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
)

# Get the ARN of the IAM Role
roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
print("IAM Role ARN:", roleArn)

# === Step 3: Create Redshift Cluster ===
try:
    print("Creating Redshift cluster...")
    redshift.create_cluster(
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,
        IamRoles=[roleArn],
        PubliclyAccessible=True
    )
except ClientError as e:
    print("Cluster may already exist:", e)

# === Step 4: Wait until cluster becomes available ===
print("Waiting for cluster to become available...")
while True:
    cluster_props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    status = cluster_props['ClusterStatus']
    if status == 'available':
        break
    print(f"Current status: {status}. Waiting 30 seconds...")
    time.sleep(30)

# Pretty print cluster properties
def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', None)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus",
                  "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k, v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

print(prettyRedshiftProps(cluster_props))

# === Step 5: Extract Endpoint for connection ===
DWH_ENDPOINT = cluster_props['Endpoint']['Address']
print(f"Cluster Endpoint: {DWH_ENDPOINT}")

# === Step 6: Open port 5439 in the VPC security group ===
try:
    vpc = ec2.Vpc(id=cluster_props['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    print("Authorizing ingress on port 5439...")
    defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
    )
except ClientError as e:
    if "InvalidPermission.Duplicate" in str(e):
        print("Ingress rule already exists.")
    else:
        raise

# === Final Output ===
print("✅ Redshift cluster created successfully!")
print(f"🔗 Endpoint: {DWH_ENDPOINT}")
print(f"🔐 IAM Role ARN: {roleArn}")
