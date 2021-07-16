package com.zho.tikautils

import java.io.{File, FileInputStream, InputStream}
import java.util
import java.util.Collections

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.input.PortableDataStream
import org.apache.tika.metadata.Metadata
import org.apache.tika.mime.MediaType
import org.apache.tika.parser.{AbstractParser, AutoDetectParser, ParseContext}
import org.apache.tika.sax.WriteOutContentHandler
import org.xml.sax.ContentHandler

/*
<?xml version="1.0" encoding="UTF-8"?>
 <mime-info>
   <mime-type type="application/hello">
          <glob pattern="*.hi"/>
   </mime-type>
 </mime-info>
 */

class TikaUtil extends AbstractParser{

  private val SUPPORTED_TYPES: util.Set[MediaType] = Collections.singleton(MediaType.application("hello"))
  val HELLO_MIME_TYPE = "application/hello"

  override def getSupportedTypes(parseContext: ParseContext): util.Set[MediaType] = {
    return SUPPORTED_TYPES
  }

  override def parse(inputStream: InputStream, contentHandler: ContentHandler, metadata: Metadata, parseContext: ParseContext): Unit = {
//    import org.apache.tika.metadata.Metadata
//    import org.apache.tika.sax.XHTMLContentHandler
//    metadata.set(Metadata.NAMESPACE_PREFIX_DELIMITER, HELLO_MIME_TYPE)
//    metadata.set("Hello", "World")
//
//    val xhtml: XHTMLContentHandler = new XHTMLContentHandler(ContentHandler, metadata)
//    xhtml.startDocument()

  }

  def tikaFunc(a: (String, PortableDataStream)): Unit ={
    val file : File = new File(a._1.drop(5))
    val myparser : AutoDetectParser = new AutoDetectParser()
    val stream : InputStream = new FileInputStream(file)
    val handler : WriteOutContentHandler = new WriteOutContentHandler(-1)
    val metadata : Metadata = new Metadata()
    val context : ParseContext = new ParseContext()

    myparser.parse(stream, handler, metadata, context)

    stream.close

    println(handler.toString())
    println("------------------------------------------------")
  }
}
object TikaUtil{
  def main1(args: Array[String]): Unit = {
//
//    val filesPath = "src\\main\\resources\\impala-3.2.pdf"
//    println(filesPath)
//    val conf = new SparkConf().setMaster("local[2]").setAppName("TikaFileParser")
//    val sc = new SparkContext(conf)
//    val fileData = sc.binaryFiles(filesPath)
//    println(fileData)
   // fileData.foreach( x => new TikaUtil().tikaFunc(x))
  }

}