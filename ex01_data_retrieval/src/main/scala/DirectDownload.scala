import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{PutObjectRequest, CreateBucketRequest, HeadBucketRequest, NoSuchBucketException}
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Configuration // IMPORTANT
import java.net.URL
import java.io.InputStream
/**
 * Télécharge un fichier depuis une URL et l’envoie directement vers MinIO (S3-compatible).
 */
object DirectDownload {
  def main(args: Array[String]): Unit = {
    // Endpoint configurable via variable d'environnement
    val minioEndpoint = sys.env.getOrElse("MINIO_ENDPOINT", "localhost:9000")
    println(s"Utilisation de l'endpoint MinIO: $minioEndpoint")

    val fileUrl = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet"
    val url = new URL(fileUrl)
    val connection = url.openConnection()
    val contentLength = connection.getContentLengthLong // Récupère la vraie taille du fichier
    val inputStream: InputStream = connection.getInputStream()

    val s3 = S3Client.builder()
      .endpointOverride(java.net.URI.create(s"http://$minioEndpoint"))
      .serviceConfiguration(S3Configuration.builder()
        .pathStyleAccessEnabled(true) // Résout l'erreur UnknownHostException
        .build())
      .credentialsProvider(
        StaticCredentialsProvider.create(
          AwsBasicCredentials.create("minio", "minio123") // Vérifiez vos IDs MinIO
        )
      )
      .region(Region.US_EAST_1)
      .build()

    // Créer le bucket s'il n'existe pas
    val bucketName = "nyc-raw"
    try {
      s3.headBucket(HeadBucketRequest.builder().bucket(bucketName).build())
      println(s"Le bucket '$bucketName' existe déjà.")
    } catch {
      case _: NoSuchBucketException =>
        println(s"Création du bucket '$bucketName'...")
        s3.createBucket(CreateBucketRequest.builder().bucket(bucketName).build())
        println(s"Bucket '$bucketName' créé avec succès.")
    }

    val request = PutObjectRequest.builder()
      .bucket(bucketName)
      .key("yellow_tripdata_2025-11.parquet")
      .build()

    println(s"Début du transfert direct depuis Cloudfront vers MinIO ($contentLength octets)...")

    s3.putObject(request, RequestBody.fromInputStream(inputStream, contentLength))

    inputStream.close()
    println("Téléchargement direct vers MinIO terminé !")
  }
}