import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{CreateBucketRequest, HeadBucketRequest, NoSuchBucketException}
import software.amazon.awssdk.services.s3.S3Configuration

import java.util.Properties
/**
 * Application Spark de validation, transformation et ingestion
 * des données NYC Taxi depuis MinIO vers un Data Warehouse.
 */
object SparkApp extends App {
  // Endpoint configurable via variable d'environnement
  val minioEndpoint = sys.env.getOrElse("MINIO_ENDPOINT", "localhost:9000")
  println(s"Utilisation de l'endpoint MinIO: $minioEndpoint")

  // Créer les buckets MinIO s'ils n'existent pas
  val s3Client = S3Client.builder()
    .endpointOverride(java.net.URI.create(s"http://$minioEndpoint"))
    .serviceConfiguration(S3Configuration.builder()
      .pathStyleAccessEnabled(true)
      .build())
    .credentialsProvider(
      StaticCredentialsProvider.create(
        AwsBasicCredentials.create("minio", "minio123")
      )
    )
    .region(Region.US_EAST_1)
    .build()

  // Créer les buckets nécessaires
  val bucketsToCreate = List("nyc-raw", "nyc-validated-b1")
  bucketsToCreate.foreach { bucketName =>
    try {
      s3Client.headBucket(HeadBucketRequest.builder().bucket(bucketName).build())
      println(s"Le bucket '$bucketName' existe déjà.")
    } catch {
      case _: NoSuchBucketException =>
        println(s"Création du bucket '$bucketName'...")
        s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build())
        println(s"Bucket '$bucketName' créé avec succès.")
    }
  }
  s3Client.close()

  // SparkSession configuré pour Minio
  val spark = SparkSession.builder()
    .appName("SparkApp")
    .config("spark.hadoop.fs.s3a.access.key", "minio")
    .config("spark.hadoop.fs.s3a.secret.key", "minio123")
    .config("spark.hadoop.fs.s3a.endpoint", s"http://$minioEndpoint")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enable", "false")
    .getOrCreate()
  import spark.implicits._

  // ---------------------------------
  // 1. Lecture des fichiers Parquet
  // ---------------------------------
  val parquetPath = "s3a://nyc-raw/yellow_tripdata_2025-11.parquet"
  val df: DataFrame = spark.read.parquet(parquetPath)

  println("Schéma initial :")
  df.printSchema()
  df.show(5)

  // ---------------------------------
  // 2. Validation des données
  // ---------------------------------
  // Exemple : id non null, date au format yyyy-MM-dd, amount > 0
  val dfValid = df.filter(
    // VendorID : 1 ou 2
    col("VendorID").isin(1, 2) &&

      // Datetimes non nulls et cohérents
      // Validation de la syntaxe et existence de la date
      to_timestamp(col("tpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss").isNotNull &&
      to_timestamp(col("tpep_dropoff_datetime"), "yyyy-MM-dd HH:mm:ss").isNotNull &&
      // Validation de la chronologie
      year(col("tpep_pickup_datetime")) === 2025 &&
      month(col("tpep_pickup_datetime")).isin(9, 10, 11) &&
      col("tpep_dropoff_datetime") > col("tpep_pickup_datetime") &&

      //Location Existante
      col("PULocationID").between(1,265) &&
      col("DOLocationID").between(1,265) &&
      // Trip_distance : > 0
      col("trip_distance") > 0 &&

      // RateCodeID : 1 à 6
      col("RateCodeID").between(1, 6) &&

      // Store_and_fwd_flag : Y ou N
      col("store_and_fwd_flag").isin("Y", "N") &&

      // Payment_type : 1 à 6
      col("payment_type").between(1, 6) &&

      col("passenger_count") >= 0 &&
      // Montants financiers cohérents
      col("fare_amount") >= 0 &&
      col("extra") >= 0 &&
      col("mta_tax") >= 0 &&
      col("improvement_surcharge") >= 0 &&
      col("tip_amount") >= 0 &&
      col("tolls_amount") >= 0 &&
      col("total_amount") >= 0
  ).cache



  // --- BRANCHE 1 : Export pour ML Engineers (MinIO) ---
  println(">>> Branche 1 : Écriture du Parquet validé pour le ML...")
  val dfInvalid = df.except(dfValid)

  println("Données valides :")
  dfValid.show(5)
  println(s"Nombre de lignes : ${dfValid.count()}")
  println("Données invalides :")
  dfInvalid.show(5)
  println(s"Nombre de lignes : ${dfInvalid.count()}")

  val outputPath = "s3a://nyc-validated-b1/yellow_tripdata_2025-11-validated.parquet"

  dfValid.write
    .mode("overwrite")
    .parquet(outputPath)

  println(s"Données validées écrites dans : $outputPath")





  // --- BRANCHE 2 : Transformation et Ingestion Data Warehouse (Postgres) ---
  println(">>> Branche 2 : Transformation temporelle et ingestion Postgres...")
  println(s"Nombre de lignes à Convertir: ${dfValid.count()}")
  val factTripsDf = dfValid.select(
    col("VendorID").as("vendor_id"),
    // Transformation des timestamps en clés de dimensions
    date_format($"tpep_pickup_datetime", "yyyyMMdd").cast("int").as("pickup_date_key"),
    date_format($"tpep_pickup_datetime", "HHmm").cast("int").as("pickup_time_key"),
    date_format($"tpep_dropoff_datetime", "yyyyMMdd").cast("int").as("dropoff_date_key"),
    date_format($"tpep_dropoff_datetime", "HHmm").cast("int").as("dropoff_time_key"),
    // Localisations
    col("PULocationID").cast("int").as("pickup_location_id"),
    col("DOLocationID").cast("int").as("dropoff_location_id"),
    // Codes
    col("RatecodeID").cast("int").as("rate_code_id"),
    col("payment_type").cast("int").as("payment_type_id"),
    // Mesures
    $"passenger_count",
    $"trip_distance",
    $"fare_amount",
    $"extra",
    $"mta_tax",
    $"tip_amount",
    $"tolls_amount",
    $"improvement_surcharge",
    $"congestion_surcharge",
    $"Airport_fee".as("airport_fee"),
    $"cbd_congestion_fee",
    $"total_amount"
  )
  // 1. Vérifier si le DataFrame est vide juste avant l'envoi à Postgres
  println(s"NB lignes à insérer dans Postgres : ${factTripsDf.count()}")

  // 2. Vérifier un échantillon des clés générées
  factTripsDf.show(5)

  // Configuration JDBC
  val jdbcUrl = "jdbc:postgresql://postgres:5432/ma_base"
  val connectionProperties = new Properties()
  connectionProperties.setProperty("user", "mon_user")
  connectionProperties.setProperty("password", "mon_password")
  connectionProperties.setProperty("driver", "org.postgresql.Driver")
  connectionProperties.setProperty("batchsize", "20000") // Optimisation
  connectionProperties.put("autocommit", "true")
  // Écriture vers la table de faits
  factTripsDf.write
    .mode(SaveMode.Append)
    .jdbc(jdbcUrl, "fact_trips", connectionProperties)
  println(s"Base de donnée: $jdbcUrl")
  println(">>> Pipeline terminé avec succès!!")


  spark.stop()
}
