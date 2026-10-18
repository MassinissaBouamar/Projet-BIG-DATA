package cytech.retrieval

import java.io.{ByteArrayInputStream, IOException, InputStream}
import java.net.{InetSocketAddress, URI}
import java.security.MessageDigest
import com.sun.net.httpserver.HttpServer
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.core.sync.{RequestBody, ResponseTransformer}
import software.amazon.awssdk.http.AbortableInputStream
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

class RetrieverSpec extends AnyFunSuite {
  private val config = Config.fromEnv(Map.empty)

  test("configuration par défaut : URL de janvier 2026, bucket nyc-raw et endpoint local") {
    assert(config.bucket == "nyc-raw")
    assert(config.key == "yellow_tripdata_2026-01.parquet")
    assert(config.endpoint == URI.create("http://localhost:9000"))
    assert(config.source.toString.endsWith("/yellow_tripdata_2026-01.parquet"))
  }

  test("configuration personnalisable et rejet des valeurs invalides") {
    val custom = Config.fromEnv(Map("S3_KEY" -> "q3/essai.parquet", "S3_REGION" -> "eu-west-1"))
    assert(custom.key == "q3/essai.parquet")
    assert(custom.region == "eu-west-1")
    for (entry <- Seq("S3_BUCKET" -> "nyc_raw", "SOURCE_URL" -> "file:///tmp/a.parquet",
      "S3_ENDPOINT" -> "http://user:secret@localhost:9000", "S3_KEY" -> "", "S3_REGION" -> "")) {
      intercept[IllegalArgumentException](Config.fromEnv(Map(entry)))
    }
    val error = intercept[IllegalArgumentException](Config.required(Map.empty, "AWS_SECRET_ACCESS_KEY"))
    assert(error.getMessage.contains("AWS_SECRET_ACCESS_KEY"))
  }

  private def withHttp(status: Int, data: Array[Byte], chunked: Boolean = false)
                      (testBody: URI => Unit): Unit = {
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/file.parquet", exchange => {
      exchange.sendResponseHeaders(status, if (chunked) 0L else data.length.toLong)
      try exchange.getResponseBody.write(data) finally exchange.close()
    })
    server.start()
    try testBody(URI.create(s"http://127.0.0.1:${server.getAddress.getPort}/file.parquet"))
    finally server.stop(0)
  }

  test("lecture HTTP sans fichier local, y compris sans Content-Length") {
    val bytes = "PAR1fixturePAR1".getBytes("UTF-8")
    for (chunked <- Seq(false, true)) withHttp(200, bytes, chunked) { uri =>
      val source = HttpSource.open(uri)
      try {
        assert(source.length == (if (chunked) None else Some(bytes.length.toLong)))
        assert(source.stream.readAllBytes().sameElements(bytes))
      } finally source.close()
    }
  }

  test("refus d'une erreur HTTP et d'une réponse HTML à la place du Parquet") {
    withHttp(404, "absent".getBytes("UTF-8")) { uri =>
      assert(intercept[IOException](HttpSource.open(uri)).getMessage.contains("404"))
    }
    withHttp(200, "<html>erreur</html>".getBytes("UTF-8")) { uri =>
      assert(intercept[IOException](HttpSource.open(uri)).getMessage.contains("PAR1"))
    }
  }

  test("refus d'écraser un objet existant et propagation d'un accès refusé") {
    val s3 = mock(classOf[S3Client])
    when(s3.headObject(any(classOf[HeadObjectRequest]))).thenReturn(HeadObjectResponse.builder().build())
    intercept[IOException](Retriever.ensureDestinationAbsent(s3, config))
    verify(s3, never()).createMultipartUpload(any(classOf[CreateMultipartUploadRequest]))
    when(s3.headObject(any(classOf[HeadObjectRequest])))
      .thenThrow(S3Exception.builder().statusCode(403).message("Forbidden").build())
    assert(intercept[S3Exception](Retriever.ensureDestinationAbsent(s3, config)).statusCode() == 403)
    when(s3.headObject(any(classOf[HeadObjectRequest])))
      .thenThrow(S3Exception.builder().statusCode(404).message("Not found").build())
    Retriever.ensureDestinationAbsent(s3, config)
  }

  private class Fixture(bytes: Array[Byte], corrupt: Boolean = false, wrongSize: Boolean = false) {
    val s3: S3Client = mock(classOf[S3Client])
    val uploaded = ArrayBuffer.empty[Byte]
    val sizes = ArrayBuffer.empty[Long]
    when(s3.createMultipartUpload(any(classOf[CreateMultipartUploadRequest])))
      .thenReturn(CreateMultipartUploadResponse.builder().uploadId("test-upload").build())
    when(s3.uploadPart(any(classOf[UploadPartRequest]), any(classOf[RequestBody])))
      .thenAnswer { invocation =>
        val request = invocation.getArgument[UploadPartRequest](0)
        val body = invocation.getArgument[RequestBody](1)
        sizes += body.contentLength()
        val stream = body.contentStreamProvider().newStream()
        try uploaded ++= stream.readAllBytes() finally stream.close()
        UploadPartResponse.builder().eTag(s"part-${request.partNumber()}").build()
      }
    when(s3.completeMultipartUpload(any(classOf[CompleteMultipartUploadRequest])))
      .thenAnswer { invocation =>
        val request = invocation.getArgument[CompleteMultipartUploadRequest](0)
        assert(request.ifNoneMatch() == "*")
        assert(request.multipartUpload().parts().asScala.map(_.partNumber().intValue()).toSeq ==
          (1 to sizes.size))
        CompleteMultipartUploadResponse.builder().eTag("completed").build()
      }
    when(s3.headObject(any(classOf[HeadObjectRequest])))
      .thenReturn(HeadObjectResponse.builder().contentLength(bytes.length.toLong + (if (wrongSize) 1 else 0))
        .eTag("completed").build())
    when(s3.getObject(any(classOf[GetObjectRequest]),
      any[ResponseTransformer[GetObjectResponse, ResponseInputStream[GetObjectResponse]]]()))
      .thenAnswer { invocation =>
        assert(invocation.getArgument[GetObjectRequest](0).ifMatch() == "completed")
        val data = bytes.clone()
        if (corrupt) data(0) = (data(0) ^ 1).toByte
        new ResponseInputStream[GetObjectResponse](GetObjectResponse.builder().build(),
          AbortableInputStream.create(new ByteArrayInputStream(data)))
      }
  }

  test("plusieurs blocs, dernier bloc court, ordre, taille et SHA-256 vérifiés") {
    val bytes = Array.tabulate[Byte](8 * 1024 * 1024 + 123)(i => (i % 251).toByte)
    val f = new Fixture(bytes)
    val result = Retriever.transfer(f.s3, config, new ByteArrayInputStream(bytes), Some(bytes.length.toLong))
    assert(f.sizes.toSeq == Seq(8L * 1024 * 1024, 123L))
    assert(f.uploaded.toArray.sameElements(bytes))
    assert(result.bytes == bytes.length)
    assert(result.sha256 == MessageDigest.getInstance("SHA-256").digest(bytes)
      .map(b => f"${b & 0xff}%02x").mkString)
    verify(f.s3, never()).abortMultipartUpload(any(classOf[AbortMultipartUploadRequest]))
  }

  test("un flux sans Content-Length est accepté") {
    val bytes = Array[Byte](1, 2, 3)
    val f = new Fixture(bytes)
    assert(Retriever.transfer(f.s3, config, new ByteArrayInputStream(bytes), None).bytes == 3)
  }

  test("source vide ou tronquée : abandon des blocs, sans finalisation") {
    for (bytes <- Seq(Array.emptyByteArray, Array[Byte](1, 2, 3))) {
      val f = new Fixture(bytes)
      intercept[IOException](Retriever.transfer(f.s3, config, new ByteArrayInputStream(bytes), Some(10L)))
      verify(f.s3).abortMultipartUpload(any(classOf[AbortMultipartUploadRequest]))
      verify(f.s3, never()).completeMultipartUpload(any(classOf[CompleteMultipartUploadRequest]))
    }
  }

  test("erreur réseau à la lecture : abandon de l'upload en cours") {
    val f = new Fixture(Array.emptyByteArray)
    val broken = new InputStream { override def read(): Int = throw new IOException("connexion coupée") }
    intercept[IOException](Retriever.transfer(f.s3, config, broken, None))
    verify(f.s3).abortMultipartUpload(any(classOf[AbortMultipartUploadRequest]))
  }

  test("conflit de finalisation : aucun écrasement et abandon des blocs temporaires") {
    val bytes = Array[Byte](1, 2, 3)
    val f = new Fixture(bytes)
    doThrow(S3Exception.builder().statusCode(412).message("PreconditionFailed").build())
      .when(f.s3).completeMultipartUpload(any(classOf[CompleteMultipartUploadRequest]))
    assert(intercept[S3Exception](Retriever.transfer(f.s3, config, new ByteArrayInputStream(bytes), None))
      .statusCode() == 412)
    verify(f.s3).abortMultipartUpload(any(classOf[AbortMultipartUploadRequest]))
    verify(f.s3, never()).deleteObject(any(classOf[DeleteObjectRequest]))
  }

  test("erreur d'upload : conserver aussi l'erreur de nettoyage si RustFS ne répond plus") {
    val bytes = Array[Byte](1, 2, 3)
    val f = new Fixture(bytes)
    doThrow(SdkClientException.create("upload interrompu"))
      .when(f.s3).uploadPart(any(classOf[UploadPartRequest]), any(classOf[RequestBody]))
    doThrow(SdkClientException.create("RustFS inaccessible"))
      .when(f.s3).abortMultipartUpload(any(classOf[AbortMultipartUploadRequest]))
    val error = intercept[SdkClientException](Retriever.transfer(f.s3, config, new ByteArrayInputStream(bytes), None))
    assert(error.getMessage == "upload interrompu")
    assert(error.getSuppressed.toSeq.map(_.getMessage) == Seq("RustFS inaccessible"))
  }

  test("une vérification de taille ou de contenu échouée ne supprime pas l'objet finalisé") {
    val bytes = Array[Byte](1, 2, 3)
    for (wrongSize <- Seq(false, true)) {
      val f = new Fixture(bytes, corrupt = !wrongSize, wrongSize = wrongSize)
      intercept[IOException](Retriever.transfer(f.s3, config, new ByteArrayInputStream(bytes), None))
      verify(f.s3, never()).abortMultipartUpload(any(classOf[AbortMultipartUploadRequest]))
      verify(f.s3, never()).deleteObject(any(classOf[DeleteObjectRequest]))
    }
  }
}
