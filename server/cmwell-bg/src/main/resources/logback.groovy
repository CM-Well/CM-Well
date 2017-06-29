import ch.qos.logback.classic.Level
import ch.qos.logback.classic.encoder.PatternLayoutEncoder

appender("RootFileAppender", RollingFileAppender) {
    file = "logs/application.log"
    append = true
    rollingPolicy(FixedWindowRollingPolicy) {
        fileNamePattern = "logs/application.log.%i"
        maxIndex = 100
    }
    triggeringPolicy(SizeBasedTriggeringPolicy) {
        maxFileSize = "20MB"
    }
    encoder(PatternLayoutEncoder) {
        pattern = "%d{yyyy-MM-dd HH:mm:ss}, %p, %c, %t, %L, %C{1}, %M %m%n"
    }
}
appender("STDOUT", ConsoleAppender) {
    encoder(PatternLayoutEncoder) {
        pattern = "%-4relative [%thread] %-5level %logger{35} - %msg %n"
    }
}
appender("BG_RED_LOG", RollingFileAppender) {
    file = "logs/red.log"
    append = true
    encoder(PatternLayoutEncoder) {
        pattern = "%date{ISO8601} [%thread] %-5level %logger{36} - %msg%n"
    }
    rollingPolicy(FixedWindowRollingPolicy) {
        fileNamePattern = "logs/red.log.%i"
        maxIndex = 21
    }
    triggeringPolicy(SizeBasedTriggeringPolicy) {
        maxFileSize = "10MB"
    }
}
appender("HEARTBIT_LOG", RollingFileAppender) {
    file = "logs/heartbit.log"
    append = true
    encoder(PatternLayoutEncoder) {
        pattern = "%date{ISO8601} [%thread] %-5level %logger{36} - %msg%n"
    }
    rollingPolicy(FixedWindowRollingPolicy) {
        fileNamePattern = "logs/red.log.%i"
        maxIndex = 1
    }
    triggeringPolicy(SizeBasedTriggeringPolicy) {
        maxFileSize = "10MB"
    }
}
logger("bg_red_log", INFO, ["BG_RED_LOG"], false)
logger("heartbit_log", INFO, ["HEARTBIT_LOG"], false)
logger("org.apache.kafka", INFO)
logger("akka", INFO)
logger("org.elasticsearch", INFO)
logger( "cmwell.bg", INFO, ["RootFileAppender"], false)
root(INFO, ["RootFileAppender"])