GCP_PROJECT_ID="ntt-test-data-bq-looker" # แทนที่ด้วย Project ID ของคุณ
GCP_REGION="asia-southeast1" # แทนที่ด้วย Region ของคุณ
DATAPROC_CLUSTER_NAME="cluster-demo-pub-dp2" # แทนที่ด้วยชื่อ Dataproc Cluster ของคุณ

# Path ไปยัง JARs บน GCS
OUTPUT_APP_JAR_NAME=$(find target/scala-2.12 -name "*.jar" -print -quit)
MAIN_CLASS_NAME="com.siebel.oracle.SimpleOracleConnectionTest"
GCS_BUCKET_NAME="dataproc-staging-asia-southeast1-434326790648-gcyeg1qw" # แทนที่ด้วยชื่อ Bucket ของคุณ
APP_JAR_GCS_PATH="gs://${GCS_BUCKET_NAME}/notebooks/jupyter/Sin/$(basename "$OUTPUT_APP_JAR_NAME")"
OJDBC_JAR_GCS_PATH="gs://${GCS_BUCKET_NAME}/ojdbc11.jar" # Path ที่ ojdbc11.jar อยู่ใน GCS

# --- Oracle DB Connection Details (ใช้ค่าที่คุณยืนยันแล้วว่าถูกต้อง) ---
DB_HOST="siebel-oracle-20250614-114148.cbxbuf3oy0lw.ap-southeast-1.rds.amazonaws.com" # RDS Endpoint
DB_PORT="1521"
DB_NAME="SIEBEL_A" # Service Name ที่ถูกต้อง
DB_USER="siebeladmin"
DB_PASSWORD="Siebel2024" # Password ที่ไม่มีอักขระพิเศษ !

JDBC_URL="jdbc:oracle:thin:@//$DB_HOST:$DB_PORT/$DB_NAME"

# **สำคัญ**: Export Environment Variables ให้ Shell เห็นก่อนรัน gcloud command
export ORACLE_URL="$JDBC_URL"
export ORACLE_USER="$DB_USER"
export ORACLE_PASSWORD="$DB_PASSWORD"

echo "Submitting Scala Spark job..."
echo "JDBC URL (from script): $JDBC_URL"

gcloud dataproc jobs submit spark \
  --cluster="$DATAPROC_CLUSTER_NAME" \
  --region="$GCP_REGION" \
  --project="$GCP_PROJECT_ID" \
  --class="$MAIN_CLASS_NAME" \
  --jars="$APP_JAR_GCS_PATH,$OJDBC_JAR_GCS_PATH" \
  --properties="spark.driver.memory=6g,spark.driver.memoryOverhead=4g,spark.executor.memory=2g,spark.executor.memoryOverhead=1g,spark.rpc.askTimeout=600s,spark.network.timeout=600s,\
spark.driver.extraClassPath=$OJDBC_JAR_GCS_PATH,\
spark.executor.extraClassPath=$OJDBC_JAR_GCS_PATH,\
spark.yarn.appMasterEnv.ORACLE_URL=$ORACLE_URL,\
spark.yarn.appMasterEnv.ORACLE_USER=$ORACLE_USER,\
spark.yarn.appMasterEnv.ORACLE_PASSWORD=$ORACLE_PASSWORD,\
spark.executorEnv.ORACLE_URL=$ORACLE_URL,\
spark.executorEnv.ORACLE_USER=$ORACLE_USER,\
spark.executorEnv.ORACLE_PASSWORD=$ORACLE_PASSWORD"