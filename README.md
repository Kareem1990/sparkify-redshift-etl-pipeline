# 🎷 Sparkify Data Warehouse Pipeline on AWS Redshift

This project builds a cloud-based **Data Warehouse** for a fictional music streaming app, **Sparkify**, using AWS Redshift, S3, and Python.

---

## 📌 Project Goal

Design and implement an ETL pipeline that:

* Extracts raw song and log data from **S3**
* Loads it into **staging tables** in Amazon **Redshift**
* Transforms it into a **star-schema** data warehouse for analytics

**The pipeline follows this order:**

0. Add your temporary AWS credentials to `dwh.cfg`
1. Run `create_aws_resources.py` to provision Redshift and IAM role
2. Copy the generated Redshift endpoint and IAM role ARN into `dwh.cfg`
3. Run `create_tables.py` to create staging, fact, and dimension tables
4. Run `etl.py` to load and transform data into final schema
5. (Optional) Use Redshift Query Editor to verify contents
6. (Optional) Run `delete_aws_resources.py` to clean up cloud resources

---

## ⚙️ Technologies Used

* **Amazon Redshift** – Cloud Data Warehouse
* **Amazon S3** – Storage for raw song & event logs (JSON)
* **IAM** – Role-based S3 access from Redshift
* **Python** – ETL scripting & automation
* **psycopg2** – PostgreSQL/Redshift connection
* **boto3** – AWS SDK for Python
* **ConfigParser** – For dynamic configuration via `dwh.cfg`

---

## 🗂️ Folder Structure

```
.
├── create_aws_resources.py       # Provisions Redshift + IAM role
├── create_tables.py              # Drops & creates all tables
├── delete_aws_resources.py       # Deletes Redshift & IAM role
├── etl.py                        # Extracts from S3, transforms, loads into Redshift
├── sql_queries.py                # SQL commands (CREATE, COPY, INSERT)
├── dwh.cfg                       # AWS credentials & config values
└── README.md                     # Project documentation
```

---

## 🚀 Pipeline Workflow

### 0. Add AWS Credentials

Before running any script, update your `dwh.cfg` file with your temporary AWS credentials:

```
[AWS]
KEY = YOUR_ACCESS_KEY
SECRET = YOUR_SECRET_KEY
SESSION = YOUR_SESSION_TOKEN
```

These credentials are required for `boto3` to interact with AWS services.

### 1. `create_aws_resources.py`

* Creates:

  * IAM role (AmazonS3ReadOnlyAccess)
  * Redshift cluster with public access
  * Opens port 5439
* Outputs `Cluster Endpoint` and `IAM Role ARN`

> 🔔 **Note:** After running this script, you must manually copy the generated **Cluster Endpoint** into the `[CLUSTER]` section and the **IAM Role ARN** into the `[IAM_ROLE]` section of your `dwh.cfg` file.
>
> 💡 **Hint:** Redshift pricing is **$0.25 per node per hour**. AWS **rounds up** to the nearest full hour if the cluster is active for more than 10 minutes. Be sure to delete the cluster promptly to avoid unnecessary charges.
>
> 🛠️ **Also:** Be sure to set `PubliclyAccessible=True` during Redshift cluster creation to avoid connection issues.

### 2. `create_tables.py`

* Connects to Redshift using values from `[CLUSTER]` in `dwh.cfg`
* Drops existing tables
* Creates:

  * Staging tables: `staging_events`, `staging_songs`
  * Fact table: `songplays`
  * Dimension tables: `users`, `songs`, `artists`, `time`

### 3. `etl.py`

* Loads JSON data from S3 into Redshift staging tables using `COPY`
* Transforms data and loads it into analytics tables using `INSERT`

### 4. (Optional) Run Queries

Use Amazon Redshift Query Editor to run validation queries, such as:

```sql
SELECT COUNT(*) FROM users;
SELECT * FROM songplays LIMIT 5;
```

You can also explore insights like:

```sql
-- Most played songs
SELECT song_id, COUNT(*) AS play_count
FROM songplays
GROUP BY song_id
ORDER BY play_count DESC
LIMIT 5;

-- Busiest day
SELECT DATE(start_time) AS day, COUNT(*) AS play_count
FROM songplays
GROUP BY day
ORDER BY play_count DESC
LIMIT 1;
```

> Example output:  
> ![Sample output](query_output.png)

### 5. `delete_aws_resources.py`

* Deletes the Redshift cluster
* Detaches policy and deletes IAM role

---

## ⭐ Star Schema

* **Fact Table**

  * `songplays`: user activity logs (listening events)

* **Dimension Tables**

  * `users`, `songs`, `artists`, `time`

* **Staging Tables**

  * `staging_events`, `staging_songs` (raw data from S3)

---

## 📝 Execution Commands

```bash
python create_aws_resources.py   # Provision IAM + Redshift
python create_tables.py          # Create schema in Redshift
python etl.py                    # Load data and transform
python delete_aws_resources.py   # Tear down resources
```

---

## 👤 Author

**Kareem Rizk**  
Cloud & Data Engineer  
🔗 [GitHub](https://github.com/Kareem1990) — 🔗 [LinkedIn](https://linkedin.com/in/kareemmagdy)