package cytech.retrieval

import java.io.{BufferedInputStream, IOException, InputStream}
import java.net.{HttpURLConnection, URI}

final class HttpSource(val stream: InputStream, val length: Option[Long],
                       connection: HttpURLConnection) extends AutoCloseable {
  override def close(): Unit = try stream.close() finally connection.disconnect()
}

object HttpSource {
  def open(uri: URI): HttpSource = {
    val connection = uri.toURL.openConnection().asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(15000)
    connection.setReadTimeout(60000)
    connection.setRequestProperty("Accept-Encoding", "identity")
    connection.setInstanceFollowRedirects(true)
    try {
      val status = connection.getResponseCode
      if (status != 200) throw new IOException(s"Téléchargement refusé : HTTP $status")
      if (Option(connection.getContentEncoding).exists(!_.equalsIgnoreCase("identity")))
        throw new IOException("Encodage HTTP inattendu : les octets Parquet doivent rester inchangés")
      val stream = new BufferedInputStream(connection.getInputStream)
      try {
        stream.mark(4)
        val magic = stream.readNBytes(4)
        stream.reset()
        if (!java.util.Arrays.equals(magic, Array[Byte](80, 65, 82, 49)))
          throw new IOException("La réponse HTTP ne commence pas par la signature Parquet PAR1")
        new HttpSource(stream, Option(connection.getContentLengthLong).filter(_ >= 0), connection)
      } catch {
        case error: Exception => stream.close(); throw error
      }
    } catch {
      case error: Exception =>
        Option(connection.getErrorStream).foreach(_.close())
        connection.disconnect()
        throw error
    }
  }
}
