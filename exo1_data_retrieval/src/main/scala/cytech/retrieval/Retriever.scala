package cytech.retrieval

import java.io.{ByteArrayInputStream, IOException, InputStream}
import java.security.MessageDigest
import java.util.Arrays
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.Using
import software.amazon.awssdk.core.sync.{RequestBody, ResponseTransformer}
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._

final case class UploadResult(bytes: Long, sha256: String)

object Retriever {
  private val PartSize = 8 * 1024 * 1024 // S3 exige au moins 5 Mio, sauf pour le dernier bloc.

  def ensureDestinationAbsent(s3: S3Client, config: Config): Unit = {
    s3.headBucket(HeadBucketRequest.builder().bucket(config.bucket).build())
    val exists = try {
      s3.headObject(HeadObjectRequest.builder().bucket(config.bucket).key(config.key).build())
      true
    } catch {
      // Un accès refusé (403) n'est pas une preuve d'absence.
      case e: S3Exception if e.statusCode() == 404 => false
    }
    if (exists) throw new IOException(
      s"Objet déjà présent : s3://${config.bucket}/${config.key}. Choisir une autre S3_KEY.")
  }

  def transfer(s3: S3Client, config: Config, input: InputStream,
               expectedLength: Option[Long]): UploadResult = {
    val uploadId = s3.createMultipartUpload(CreateMultipartUploadRequest.builder()
      .bucket(config.bucket).key(config.key).contentType("application/octet-stream").build()).uploadId()
    var completed = false
    try {
      val digest = MessageDigest.getInstance("SHA-256")
      val parts = ArrayBuffer.empty[CompletedPart]
      val buffer = new Array[Byte](PartSize)
      var total = 0L
      var count = input.readNBytes(buffer, 0, buffer.length)
      while (count > 0) {
        if (parts.size >= 10000) throw new IOException("Limite S3 de 10 000 blocs dépassée")
        digest.update(buffer, 0, count)
        total += count
        // Le bloc reste en mémoire jusqu'à la réponse : le SDK peut le rejouer en cas d'erreur réseau.
        val body = RequestBody.fromInputStream(new ByteArrayInputStream(buffer, 0, count), count.toLong)
        val response = s3.uploadPart(UploadPartRequest.builder().bucket(config.bucket)
          .key(config.key).uploadId(uploadId).partNumber(parts.size + 1).build(), body)
        parts += CompletedPart.builder().partNumber(parts.size + 1).eTag(response.eTag()).build()
        count = input.readNBytes(buffer, 0, buffer.length)
      }
      if (total == 0 || expectedLength.exists(_ != total))
        throw new IOException(s"Source vide ou tronquée : $total octets reçus, attendu : $expectedLength")

      val sourceHash = digest.digest()
      s3.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
        .bucket(config.bucket).key(config.key).uploadId(uploadId)
        // Protection atomique si un autre client crée la même clé pendant le transfert.
        .ifNoneMatch("*")
        .multipartUpload(CompletedMultipartUpload.builder().parts(parts.asJava).build()).build())
      completed = true

      val head = s3.headObject(HeadObjectRequest.builder().bucket(config.bucket).key(config.key).build())
      if (head.contentLength() != total) throw new IOException("Vérification échouée : taille S3 différente")
      // L'ETag multipart n'est pas le SHA-256 du fichier : on relit réellement l'objet.
      val remoteHash = MessageDigest.getInstance("SHA-256")
      var remoteSize = 0L
      Using.resource(s3.getObject(GetObjectRequest.builder().bucket(config.bucket).key(config.key)
        .ifMatch(head.eTag()).build(), ResponseTransformer.toInputStream[GetObjectResponse]())) { remote =>
        var n = remote.read(buffer)
        while (n != -1) {
          remoteHash.update(buffer, 0, n)
          remoteSize += n
          n = remote.read(buffer)
        }
      }
      if (remoteSize != total || !Arrays.equals(sourceHash, remoteHash.digest()))
        throw new IOException("Vérification échouée : contenu S3 différent (SHA-256)")
      UploadResult(total, sourceHash.map(b => f"${b & 0xff}%02x").mkString)
    } catch {
      case error: Exception =>
        if (!completed) {
          // Nettoyage uniquement des blocs temporaires créés par CETTE exécution.
          try s3.abortMultipartUpload(AbortMultipartUploadRequest.builder()
            .bucket(config.bucket).key(config.key).uploadId(uploadId).build())
          catch { case cleanup: Exception => error.addSuppressed(cleanup) }
        }
        // Après finalisation, conserver l'objet même si sa vérification échoue.
        throw error
    }
  }
}
