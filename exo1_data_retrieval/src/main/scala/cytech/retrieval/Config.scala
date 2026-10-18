package cytech.retrieval

import java.net.URI

final case class Config(source: URI, endpoint: URI, region: String, bucket: String, key: String)

object Config {
  private val defaultSource =
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet"

  def required(env: Map[String, String], name: String): String =
    env.get(name).filter(_.trim.nonEmpty).getOrElse(
      throw new IllegalArgumentException(s"Variable d'environnement manquante : $name")
    )

  def fromEnv(env: Map[String, String]): Config = {
    def value(name: String, default: String): String = env.getOrElse(name, default).trim
    def uri(name: String, default: String): URI = {
      val result = URI.create(value(name, default))
      require(Set("http", "https").contains(result.getScheme) && result.getHost != null &&
        result.getUserInfo == null && result.getFragment == null && result.getQuery == null,
        s"$name doit être une URL HTTP(S) sans identifiants, query ni fragment")
      result
    }
    val source = uri("SOURCE_URL", defaultSource)
    val endpoint = uri("S3_ENDPOINT", "http://localhost:9000")
    require(Option(endpoint.getPath).forall(p => p.isEmpty || p == "/"),
      "S3_ENDPOINT ne doit pas contenir de chemin")
    val bucket = value("S3_BUCKET", "nyc-raw")
    require(bucket.matches("[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]") &&
      !bucket.contains("..") && !bucket.matches("[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+"),
      "S3_BUCKET invalide : utiliser par exemple nyc-raw, sans underscore")
    val key = value("S3_KEY", source.getPath.split('/').lastOption.getOrElse(""))
    require(key.nonEmpty && key.endsWith(".parquet"), "S3_KEY doit désigner un fichier .parquet")
    val region = value("S3_REGION", "us-east-1")
    require(region.nonEmpty, "S3_REGION ne doit pas être vide")
    Config(source, endpoint, region, bucket, key)
  }
}
