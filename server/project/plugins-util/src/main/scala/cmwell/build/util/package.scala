package cmwell.build

import java.io.InputStream

package object util {
  def resourceFromJarAsIStream(filename: String): InputStream = {
    val jarfile = {
      val r = this.getClass.getProtectionDomain.getCodeSource.getLocation.getFile
      if (r.head == '/') r.tail else r
    }
    val zip = new java.util.zip.ZipFile(jarfile)
    val ze = {
      val rv = zip.getEntry(filename)
      if (rv == null) {
        val toggleSlash = if (filename.head == '/') filename.drop(1) else s"/$filename"
        zip.getEntry(toggleSlash)
      } else rv
    }
    zip.getInputStream(ze)
  }
}
