import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Configuration // IMPORTANT
import java.net.URL
import java.io.InputStream

object DirectDownload {
  def main(args: Array[String]): Unit = {

    val fileUrl = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet"
    val url = new URL(fileUrl)
    val connection = url.openConnection()
    val contentLength = connection.getContentLengthLong // Récupère la vraie taille du fichier
    val inputStream: InputStream = connection.getInputStream()

    val s3 = S3Client.builder()
      .endpointOverride(java.net.URI.create("http://localhost:9000"))
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

    val request = PutObjectRequest.builder()
      .bucket("nyc-raw")
      .key("yellow_tripdata_2025-11.parquet")
      .build()

    println(s"Début du transfert direct depuis Cloudfront vers MinIO ($contentLength octets)...")

    s3.putObject(request, RequestBody.fromInputStream(inputStream, contentLength))

    inputStream.close()
    println("Téléchargement direct vers MinIO terminé !")
  }
}