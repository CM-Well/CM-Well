//
// Built on Tue Mar 29 07:43:58 UTC 2016 by logback-translator
// For more information on configuration files in Groovy
// please see http://logback.qos.ch/manual/groovy.html

// For assistance related to this tool or configuration files
// in general, please contact the logback user mailing list at
//    http://qos.ch/mailman/listinfo/logback-user

// For professional support please see
//   http://www.qos.ch/shop/products/professionalSupport

import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.core.FileAppender

import static ch.qos.logback.classic.Level.DEBUG

appender("FileAppender", FileAppender) {
  file = "target/logs/application.log"
  append = true
  encoder(PatternLayoutEncoder) {
    pattern = "[%p] %d{ISO8601} [%t] %C{1}\\(%F:%L\\) %m%n"
  }
}
root(DEBUG, ["FileAppender"])