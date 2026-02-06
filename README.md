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
