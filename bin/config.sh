#!/bin/zsh

# Set variables and environment variables for GCP 
export PROJECT_ID=sales-batch-data-analysis
export REGION=europe-west2
# Set the variable (use the same PROJECT_ID from your batch script)
export CLOUDSDK_QUOTA_PROJECT="$PROJECT_ID"

# Define network and subnetwork URIs
NET_URI="projects/$PROJECT_ID/global/networks/dataproc-serverless-vpc"
SUBNET_URI="projects/$PROJECT_ID/regions/$REGION/subnetworks/dataproc-serverless-subnet"

STAGING_BUCKET=gs://$PROJECT_ID-staging
CONFIG_BUCKET=gs://$PROJECT_ID-config
SERVICE_ACCOUNT_KEY=../key/sales-batch-data-analysis.json

# Set environment variables for the JAR file paths
ICEBERG_LOCAL_JAR=../source/lib/iceberg/iceberg-spark-runtime-3.5_2.12-1.6.1.jar
ICEBERG_SPARK_RUNTIME_JAR=$CONFIG_BUCKET/iceberg/config/iceberg-spark-runtime-3.5_2.12-1.6.1.jar
ICEBERG_BIGQUERY_CATALOG_JAR=gs://spark-lib/bigquery/iceberg-bigquery-catalog-1.6.1-1.0.1-beta.jar

# Authenticate and set GCP project and region
echo "1. Service Account Authentication..."
if [ -f "$SERVICE_ACCOUNT_KEY" ]; then
    gcloud auth activate-service-account --key-file="$SERVICE_ACCOUNT_KEY"
    gcloud config set project "$PROJECT_ID"
    gcloud config set compute/region "$REGION"
else
    echo "ERROR: The Service Account at $SERVICE_ACCOUNT_KEY not found"
    exit 1
fi
echo "Autentication completed."

# Set the quota project for billing purposes
echo "2. Setting the cuota project..."
gcloud auth application-default set-quota-project $CLOUDSDK_QUOTA_PROJECT
echo "Quota project set to $CLOUDSDK_QUOTA_PROJECT."  

# Upload the Iceberg Spark runtime JAR to the specified GCS path
echo "3. Uploading Iceberg Spark runtime JAR to GCS..."
if [ -f "$ICEBERG_LOCAL_JAR" ]; then
    gsutil ls "$ICEBERG_SPARK_RUNTIME_JAR" > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo "Iceberg Spark runtime JAR already exists in GCS, it wont be copied again."
    else
        gsutil cp "$ICEBERG_LOCAL_JAR" "$ICEBERG_SPARK_RUNTIME_JAR"
        echo "Upload completed."
    fi
else
    echo "ERROR: The Iceberg Spark runtime JAR. $ICEBERG_LOCAL_JAR not found"
    exit 1
fi  

# Submit the Dataproc batch job with the specified JAR files
echo "4. Launching PySpark Serverless job..."
# Generate a unique job ID using the current timestamp
JOB_ID="pyspark-sales-processor-$(date +%s)"
gcloud dataproc batches submit pyspark ../source/sales_analysis_job.py \
    --project="$PROJECT_ID" \
    --region="$REGION" \
    --batch="$JOB_ID" \
    --version=2.2 \
    --subnet="$SUBNET_URI" \
    --deps-bucket="$STAGING_BUCKET" \
    --properties=spark.executor.instances=2 \
    --jars="$ICEBERG_SPARK_RUNTIME_JAR","$ICEBERG_BIGQUERY_CATALOG_JAR" 1>&1
 if [ $? -eq 0 ]; then
    echo "4. Job sent with ID: $JOB_ID"
    echo "To check the job status, execute :"
    echo "gcloud dataproc batches describe $JOB_ID --region=$REGION"
else
    echo "ERROR: There was an error submitting the Dataproc batch job."
    exit 1
fi
echo "Process completed."
