package cytech.retrieval

import java.time.Duration
import scala.util.Using
import scala.util.control.NonFatal
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.S3Exception

object Main {
  private def reportCleanupFailure(error: Throwable): Unit =
    if (error.getSuppressed.nonEmpty)
      System.err.println("Le nettoyage multipart a aussi échoué : vérifier les uploads incomplets dans RustFS.")

  def main(args: Array[String]): Unit = {
    try {
      require(args.isEmpty, "La configuration utilise les variables d'environnement (voir README.md)")
      val env = sys.env
      val config = Config.fromEnv(env)
      val credentials = AwsBasicCredentials.create(
        Config.required(env, "AWS_ACCESS_KEY_ID"), Config.required(env, "AWS_SECRET_ACCESS_KEY"))
      Using.resource(S3Client.builder().endpointOverride(config.endpoint)
        .region(Region.of(config.region)).forcePathStyle(true)
        .credentialsProvider(StaticCredentialsProvider.create(credentials))
        .httpClientBuilder(UrlConnectionHttpClient.builder()
          .connectionTimeout(Duration.ofSeconds(15)).socketTimeout(Duration.ofSeconds(60)))
        .overrideConfiguration(c => c.apiCallAttemptTimeout(Duration.ofMinutes(3))
          .apiCallTimeout(Duration.ofMinutes(10)))
        .build()) { s3 =>
        Retriever.ensureDestinationAbsent(s3, config)
        println(s"Transfert vers s3://${config.bucket}/${config.key} ...")
        val result = Using.resource(HttpSource.open(config.source)) { source =>
          Retriever.transfer(s3, config, source.stream, source.length)
        }
        println(s"Upload vérifié : ${result.bytes} octets ; SHA-256 : ${result.sha256}")
      }
    } catch {
      case e: S3Exception =>
        // Ne pas afficher les requêtes signées ou les identifiants.
        System.err.println(s"Erreur S3 HTTP ${e.statusCode()} : vérifier RustFS, le bucket, les droits " +
          "et l'absence d'objet existant (409/412). Aucun objet existant n'est supprimé.")
        reportCleanupFailure(e)
        sys.exit(1)
      case NonFatal(e) =>
        System.err.println(s"Échec : ${e.getClass.getSimpleName} : ${e.getMessage}")
        reportCleanupFailure(e)
        sys.exit(1)
    }
  }
}
