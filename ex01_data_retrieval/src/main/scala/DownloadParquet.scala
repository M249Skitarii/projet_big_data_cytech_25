import java.io.File
import java.net.URL
import java.nio.file.{Files, Paths, StandardCopyOption}
/**
 * Objet permettant de télécharger un fichier Parquet depuis une URL
 * et de l'enregistrer localement sur le disque.
 *
 */
object DownloadParquet {
  /**
   * Point d’entrée du programme.
   */
  def main(args: Array[String]): Unit = {
    
    val url =
      "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet"
    val destFile = new File("data/raw/yellow_tripdata_2025-11.parquet")

    // Crée le dossier parent s'il n'existe pas
    destFile.getParentFile.mkdirs()

    // Téléchargement du fichier
    val urlStream = new URL(url).openStream()
    try {
        Files.copy(urlStream, destFile.toPath, StandardCopyOption.REPLACE_EXISTING)
    } finally {
        urlStream.close()
      }

    println("Téléchargement terminé")
  }
}
