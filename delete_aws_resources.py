import boto3
import configparser
from botocore.exceptions import ClientError
from utils import reset_placeholders  # ✅ لإعادة placeholders بعد الحذف

# === Load AWS credentials from .aws_credentials ===
creds = configparser.ConfigParser()
creds.read('.aws_credentials', encoding='utf-8')

KEY = creds.get('AWS', 'KEY')
SECRET = creds.get('AWS', 'SECRET')
REGION = 'us-west-2'

# === Load remaining config from dwh.cfg ===
config = configparser.ConfigParser()
config.read('dwh.cfg', encoding='utf-8')

DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

# === Create AWS clients ===
redshift = boto3.client('redshift',
                        region_name=REGION,
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET)

iam = boto3.client('iam',
                   region_name=REGION,
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET)

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

# === Reset config placeholders in dwh.cfg ===
reset_placeholders("dwh.cfg")
print("🔄 Config file placeholders reset after deletion.")
print("✅ Done cleaning up AWS resources.")
