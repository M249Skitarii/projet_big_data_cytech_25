Le code minimal pour faire fonctionner un code avec Minio :
```scala
import org.apache.spark.sql.{SparkSession, DataFrame}

object SparkApp extends App {
  val spark = SparkSession.builder()
    .appName("SparkApp")
    .master("local")
    .config("fs.s3a.access.key", "minio")
    .config("fs.s3a.secret.key", "minio123")
    .config("fs.s3a.endpoint", "http://localhost:9000/") // A changer lors du déploiement
    .config("fs.s3a.path.style.access", "true")
    .config("fs.s3a.connection.ssl.enable", "false")
    .config("fs.s3a.attempts.maximum", "1")
    .config("fs.s3a.connection.establish.timeout", "6000")
    .config("fs.s3a.connection.timeout", "5000")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

}
```

## Modalités de rendu

1. Pull Request vers la branch `master`
2. Dépot du rapport et du code source zippé dans cours.cyu.fr (Les accès seront bientôt ouverts)

Date limite de rendu : 7 février 2026



## Commands

/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --class Branch1.SparkApp \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /home/root/ex02_data_ingestion_2.13-0.1.0-SNAPSHOT.jar


## TODO 

Comment Code with pyment 
Clean code
Clean Dashboard

## Results

dashboard.py : 
<img width="1888" height="651" alt="image" src="https://github.com/user-attachments/assets/029ae2f1-55ee-4616-89e6-17241ff0b25e" />

data_visualisation.py :
<img width="1916" height="513" alt="image" src="https://github.com/user-attachments/assets/86d616df-51d8-437e-a89e-778a4c1e2b90" />
<img width="1904" height="839" alt="image" src="https://github.com/user-attachments/assets/28aa64dd-11a4-46a1-8228-ccfcde55cabb" />
<img width="1891" height="848" alt="image" src="https://github.com/user-attachments/assets/8863681a-bad5-4d35-8428-f5fe71b17f60" />

Ex05 prediction streamlit_page.py : 
<img width="694" height="695" alt="image" src="https://github.com/user-attachments/assets/79bd92e8-30aa-4e26-bdf2-865df7ce1fc9" />

Ex06 Airflow pipeline flow
![image](https://github.com/user-attachments/assets/2ac5c1ac-e93d-4140-a707-28edfd739453)


