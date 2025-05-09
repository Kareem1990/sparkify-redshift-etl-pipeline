import boto3
import configparser
from botocore.exceptions import ClientError

# === Load config ===
config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')
SESSION = config.get('AWS', 'SESSION')
REGION = 'us-west-2'

DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

# === Create AWS Clients ===
redshift = boto3.client('redshift',
                        region_name=REGION,
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET,
                        aws_session_token=SESSION)

iam = boto3.client('iam',
                   region_name=REGION,
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET,
                   aws_session_token=SESSION)

# === Delete Redshift Cluster ===
try:
    print("🔻 Deleting Redshift Cluster...")
    redshift.delete_cluster(
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        SkipFinalClusterSnapshot=True
    )
except ClientError as e:
    print(f"⚠️ Error deleting cluster: {e}")

# === Detach Policy from IAM Role ===
try:
    print("🔗 Detaching AmazonS3ReadOnlyAccess policy from IAM Role...")
    iam.detach_role_policy(
        RoleName=DWH_IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )
except ClientError as e:
    print(f"⚠️ Error detaching policy: {e}")

# === Delete IAM Role ===
try:
    print("❌ Deleting IAM Role...")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
except ClientError as e:
    print(f"⚠️ Error deleting IAM role: {e}")

print("✅ Done cleaning up AWS resources.")
