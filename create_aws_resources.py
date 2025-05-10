# pip install configupdater
print("✅ YOU ARE RUNNING THE CORRECT FILE")
from utils import reset_placeholders
import boto3
import json
import time
import pandas as pd
from botocore.exceptions import ClientError
from string import Template
import re
import configparser

# === Load credentials ===
creds = configparser.ConfigParser()
creds.read('.aws_credentials')
KEY = creds.get('AWS', 'KEY')
SECRET = creds.get('AWS', 'SECRET')
REGION = "us-west-2"

reset_placeholders("dwh.cfg")

# === Step 0: Load template config and extract static values ===
with open("dwh.cfg", "r", encoding="utf-8") as f:
    cfg_template = Template(f.read())

def extract_value(content, key):
    match = re.search(rf"{key}=(.+)", content)
    return match.group(1).strip().split()[0] if match else None

DWH_CLUSTER_TYPE = extract_value(cfg_template.template, "DWH_CLUSTER_TYPE")
DWH_NUM_NODES = int(extract_value(cfg_template.template, "DWH_NUM_NODES"))
DWH_NODE_TYPE = extract_value(cfg_template.template, "DWH_NODE_TYPE")
DWH_CLUSTER_IDENTIFIER = extract_value(cfg_template.template, "DWH_CLUSTER_IDENTIFIER")
DWH_DB = extract_value(cfg_template.template, "DWH_DB")
DWH_DB_USER = extract_value(cfg_template.template, "DWH_DB_USER")
DWH_DB_PASSWORD = extract_value(cfg_template.template, "DWH_DB_PASSWORD")
DWH_PORT = int(extract_value(cfg_template.template, "DWH_PORT"))
DWH_IAM_ROLE_NAME = extract_value(cfg_template.template, "DWH_IAM_ROLE_NAME")

# === Step 1: Create AWS clients ===
ec2 = boto3.resource('ec2', region_name=REGION,
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET)

iam = boto3.client('iam', region_name=REGION,
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET)

redshift = boto3.client('redshift', region_name=REGION,
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET)

# === Step 2: Create IAM Role ===
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

print("Attaching AmazonS3ReadOnlyAccess policy...")
iam.attach_role_policy(
    RoleName=DWH_IAM_ROLE_NAME,
    PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
)

roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
print("IAM Role ARN:", roleArn)

# === Step 3: Create Redshift Cluster ===
try:
    print("Creating Redshift cluster...")
    redshift.create_cluster(
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=DWH_NUM_NODES,
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,
        IamRoles=[roleArn],
        PubliclyAccessible=True
    )
except ClientError as e:
    print("Cluster may already exist:", e)

# === Step 4: Wait for Redshift cluster to be available ===
print("Waiting for cluster to become available...")
while True:
    try:
        cluster_props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        status = cluster_props['ClusterStatus']
        if status == 'available':
            break
        print(f"Current status: {status}. Waiting 30 seconds...")
        time.sleep(30)
    except redshift.exceptions.ClusterNotFoundFault:
        print("Cluster not found yet. Retrying in 30 seconds...")
        time.sleep(30)

def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', None)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus",
                  "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k, v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

print(prettyRedshiftProps(cluster_props))

DWH_ENDPOINT = cluster_props['Endpoint']['Address']
print(f"Cluster Endpoint: {DWH_ENDPOINT}")

# === Step 5: Open port 5439 ===
try:
    vpc = ec2.Vpc(id=cluster_props['VpcId'])
    sgs = list(vpc.security_groups.all())
    if not sgs:
        raise Exception("No security groups found in VPC.")
    defaultSg = sgs[0]

    print("Authorizing ingress on port 5439...")
    defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=DWH_PORT,
        ToPort=DWH_PORT
    )
except ClientError as e:
    if "InvalidPermission.Duplicate" in str(e):
        print("Ingress rule already exists.")
    else:
        raise

# === Step 6: Write final config with values ===
filled = cfg_template.substitute(
    redshift_host=DWH_ENDPOINT,
    iam_role_arn=roleArn
)

with open("dwh.cfg", "w", encoding="utf-8") as f:
    f.write(filled)

# === Step 7: Validate replacement worked ===
with open("dwh.cfg", "r", encoding="utf-8") as f:
    content = f.read()
    assert "${" not in content, "❌ Placeholder not replaced properly!"
    assert "redshift_host" not in content, "❌ HOST placeholder still exists!"
    assert "iam_role_arn" not in content, "❌ IAM_ROLE_ARN placeholder still exists!"

print("✅ All placeholders replaced successfully.")
print("✅ Config file updated dynamically with actual values!")
print(f"🔗 Endpoint: {DWH_ENDPOINT}")
print(f"🔐 IAM Role ARN: {roleArn}")
