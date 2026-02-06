#!/bin/bash

if [ "$SPARK_MODE" = "master" ]; then
  # Installer SSH et préparer le service
  apt-get update && \
  apt-get install -y openssh-server && \
  mkdir -p /var/run/sshd
  # Définir le mot de passe root
  echo "root:1" | chpasswd
  # Modifier sshd_config pour autoriser root et mot de passe
  sed -i 's/^#PermitRootLogin.*/PermitRootLogin yes/' /etc/ssh/sshd_config && \
  sed -i 's/^#PasswordAuthentication.*/PasswordAuthentication yes/' /etc/ssh/sshd_config
  #Ouvrir le SSH
  /usr/sbin/sshd

  #SPARK
  /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
    --host spark-master \
    --port 7077 \
    --webui-port 8080
elif [ "$SPARK_MODE" = "worker" ]; then
  /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
    ${SPARK_MASTER_URL} \
    --memory ${SPARK_WORKER_MEMORY:-1G} \
    --cores ${SPARK_WORKER_CORES:-1}
else
  echo "Error: SPARK_MODE must be set to 'master' or 'worker'"
  exit 1
fi