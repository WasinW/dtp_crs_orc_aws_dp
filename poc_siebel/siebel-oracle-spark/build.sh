sbt clean
sbt assembly

OUTPUT_APP_JAR_NAME=$(find target/scala-2.12 -name "*.jar" -print -quit)
GCS_BUCKET_NAME="dataproc-staging-asia-southeast1-434326790648-gcyeg1qw" # แทนที่ด้วยชื่อ Bucket ของคุณ
GCS_NOTEBOOKS_PATH="${GCS_BUCKET_NAME}/notebooks/jupyter/Sin"
gsutil cp "$OUTPUT_APP_JAR_NAME" "gs://${GCS_NOTEBOOKS_PATH}/"

