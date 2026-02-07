# Activer le mode strict pour arrêter en cas d'erreur
$ErrorActionPreference = "Stop"

# Remonter d'un cran par rapport au script (le parent direct)
$projectDir = Split-Path -Parent $PSScriptRoot
Write-Host "Demarrage du service Airflow..." -ForegroundColor Yellow

# # Arrêt des services existants
# Write-Host "Arret des services existants..." -ForegroundColor Yellow
# docker compose down

# # Reconstruction des images Docker
# Write-Host "Reconstruction des images Docker (avec boto3)..." -ForegroundColor Yellow
# docker compose build --no-cache

# # Démarrage des services
# Write-Host "Demarrage des services avec Airflow..." -ForegroundColor Yellow
# docker compose up -d

# Write-Host "Attente du demarrage d'Airflow (60 secondes)..." -ForegroundColor Yellow
# Start-Sleep -Seconds 60

# Copier le DAG vers le conteneur Airflow
Write-Host "Copie du DAG vers Airflow..." -ForegroundColor Yellow
$tmpDag = Join-Path $env:TEMP "dag_copy.py"
Copy-Item "$projectDir\ex06_airflow\nyc_taxi_pipeline.py" -Destination $tmpDag
docker cp $tmpDag airflow:/opt/airflow/dags/nyc_taxi_pipeline.py
docker exec -u root airflow chown airflow:root /opt/airflow/dags/nyc_taxi_pipeline.py
docker exec airflow chmod 750 /opt/airflow/dags/nyc_taxi_pipeline.py
Remove-Item $tmpDag

# Copier le code ex01 et ex02 vers Airflow
Write-Host "Copie des projets Scala vers Airflow..." -ForegroundColor Yellow
docker cp "$projectDir\ex01_data_retrieval" airflow:/opt/airflow/
docker cp "$projectDir\ex02_data_ingestion" airflow:/opt/airflow/
docker exec -u root airflow chown -R airflow:root /opt/airflow/ex01_data_retrieval
docker exec -u root airflow chown -R airflow:root /opt/airflow/ex02_data_ingestion

#Creation reset d'admin
docker exec airflow airflow users delete --username admin
docker exec airflow airflow users create `
  --username admin `
  --firstname admin `
  --lastname admin `
  --role Admin `
  --email admin@admin.com `
  --password admin
Write-Host "Attente de la detection du DAG par Airflow (30 secondes)..." -ForegroundColor Yellow
Start-Sleep -Seconds 30

# Message final
Write-Host "Configuration terminee" -ForegroundColor Green
Write-Host "Interface Airflow disponible sur: http://localhost:8082" -ForegroundColor Green
Write-Host "Username: admin" -ForegroundColor Green
Write-Host "Password: admin" -ForegroundColor Green
Write-Host ""
Write-Host "Pour activer le DAG :" -ForegroundColor Yellow
Write-Host "1. Ouvrir http://localhost:8082"
Write-Host "2. Se connecter avec admin/admin"
Write-Host "3. Activer le DAG 'nyc_taxi_pipeline'"
Write-Host "4. Cliquer sur 'Trigger DAG' pour l'executer manuellement"
