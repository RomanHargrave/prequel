package net.noerd.prequel

import java.io.InputStream
import java.util.Properties
import org.slf4j.LoggerFactory

/**
 * Created with IntelliJ IDEA.
 * User: GiovanniCaruso
 * Date: 10/26/13
 * Time: 6:40 PM
 */
object SQLLogHandler {

  val sqllogger = LoggerFactory.getLogger("SQLLOG")
  var toLog = true

  var toTime = true

  val loader: ClassLoader = this.getClass.getClassLoader

  CloseUtil.closeAfterUse(loader.getResourceAsStream("prequelous.properties")) {
    in =>
      val props = new Properties(System.getProperties)
      if (in != null) {
        props.load(in)
        if (props.getProperty("prequelous.text").equalsIgnoreCase("false")) toLog = false
        if (props.getProperty("prequelous.time").equalsIgnoreCase("false")) toTime = false
      }
  }

  def createLogEntry(sql: String, params: String, time: String) = {
    if(toLog){
      val log = new StringBuilder(sql)
      log.append(params)
      if (toTime)
        log.append(time)
      sqllogger.info(log.toString)
    }
  }


}
