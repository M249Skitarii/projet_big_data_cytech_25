# Use to upload and launch streamlit on the container
# if the port is not 8501 on launching streamlit, you might want to restart the container.


$LOCAL_SRC = "src"
$REMOTE_USER = "root"
$REMOTE_HOST = "localhost"
$REMOTE_PATH = "/app"
$STREAMLIT_CONTAINER = "streamlit-page"
$SSH_PORT = 2223
# Upload avec scp
Write-Host "Uploading ${LOCAL_SRC} to ${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_PATH} ..."
scp -P $SSH_PORT -r $LOCAL_SRC "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_PATH}"
if ($LASTEXITCODE -ne 0) { Write-Host "Upload failed"; exit 1 }
Write-Host "Upload completed"

# Exécution dans le container STREAMLIT
$REMOTE_FILE_PATH = $REMOTE_PATH + "/" + $LOCAL_SRC +"/streamlit_page.py"
Write-Host "Running STREAMLIT job in container ${STREAMLIT_CONTAINER} ..."
docker exec ${STREAMLIT_CONTAINER} bash -c "cd /app && uv run streamlit run ${REMOTE_FILE_PATH}"
if ($LASTEXITCODE -ne 0) { Write-Host "Submit failed"; exit 1 }

Write-Host "STREAMLIT job finished successfully"
