# Use to upload and run model training


$LOCAL_SRC = "src"
$REMOTE_USER = "root"
$REMOTE_HOST = "localhost"
$REMOTE_PATH = "/app"
$SPARK_CONTAINER = "spark-master"
$SSH_PORT = 2222
# Upload avec scp
Write-Host "Uploading ${LOCAL_SRC} to ${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_PATH} ..."
scp -P $SSH_PORT -r $LOCAL_SRC "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_PATH}"
if ($LASTEXITCODE -ne 0) { Write-Host "Upload failed"; exit 1 }
Write-Host "Upload completed"

# Exécution dans le container Spark
$REMOTE_FILE_PATH = $REMOTE_PATH + "/" + $LOCAL_SRC + "/model.py"
Write-Host "Running Spark job in container ${SPARK_CONTAINER} ..."
docker exec ${SPARK_CONTAINER} bash -c "cd /app && uv run /opt/spark/bin/spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.6 ${REMOTE_FILE_PATH}"
if ($LASTEXITCODE -ne 0) { Write-Host "Submit failed"; exit 1 }

Write-Host "Spark job finished successfully"