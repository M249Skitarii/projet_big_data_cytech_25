import software.amazon.awssdk.auth.credentials._
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3._
import software.amazon.awssdk.services.s3.model._
import java.nio.file.Paths
/**
 * Upload un fichier local vers un bucket MinIO (S3-compatible).
 */
object UploadToMinio {
    /**
   * Point d’entrée du programme.
   */
  def main(args: Array[String]): Unit = {

    val s3 = S3Client.builder()
      .endpointOverride(java.net.URI.create("http://127.0.0.1:9000"))
      .credentialsProvider(
        StaticCredentialsProvider.create(
          AwsBasicCredentials.create("minio", "minio123")
        )
      )
      .region(Region.US_EAST_1)
      .build()
    try {
      val request = PutObjectRequest.builder()
        .bucket("nyc-raw")
        .key("yellow_tripdata_2025-11.parquet")
        .build()

      s3.putObject(
        request,
        Paths.get("..","data/raw/yellow_tripdata_2025-11.parquet")
      )
    } finally {
      s3.close()
    }

    println("Upload vers MinIO terminé")
  }
}
