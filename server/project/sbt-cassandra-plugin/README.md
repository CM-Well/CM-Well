sbt-cassandra-plugin
====================

This is a work in progress project.  The goal is to allow launching [Cassandra](http://cassandra.apache.org) during tests, and test your application against it (much like the [plugin for maven](http://mojo.codehaus.org/cassandra-maven-plugin)).
at this pre-mature phase, only the very basic functionality works (and only on linux/unix). API is not final, and might (probably will) change down the road.
However, the plugin is already usable as is.

## Installation ##
Add the following to your `project/plugins.sbt` file:
```scala
resolvers += "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases"

addSbtPlugin("com.github.hochgi" % "sbt-cassandra-plugin" % "0.6.2")
```

## Usage ##
### Basic: ###
```scala
import com.github.hochgi.sbt.cassandra._

CassandraPlugin.cassandraSettings

test in Test <<= (test in Test).dependsOn(startCassandra)
```
### Advanced: ##
To choose a specific version of cassandra (default is 2.1.2), you can use:
```scala
cassandraVersion := "2.1.2"
```
cassandra now shuts down by default when tests are done. to disable this behavior, set:
```scala
stopCassandraAfterTests := false
```
cassandra will also clean it's data by default when it stops (after tests or when invoking `stopCassandra` task explicitly). to disable this behavior, set:
```scala
cleanCassandraAfterStop := false
```
to use special configuration files suited for your use case, use:
```scala
cassandraConfigDir := "/path/to/your/conf/dir"
```
to intialize cassandra with your custom cassandra-cli commands, use:
```scala
cassandraCliInit := "/path/to/cassandra-cli/commands/file"
```
to intialize cassandra with your custom cql commands, use:
```scala
cassandraCqlInit := "/path/to/cassandra-cql/commands/file"
```
to change cassandra rpc port (note: even if you change the port on the configuration, this is the port number that will be used), use:
```scala
cassandraPort := "PORT_NUMBER"
```
timeout for waiting on cassandra to start (default is 20 seconds) can be configured with property (in seconds):
```scala
cassandraStartDeadline := 10
```
also, you may override any other configuration, e.g:
```scala
configMappings +=  "auto_snapshot" -> true
configMappings ++= Seq(
  "rpc_server_type" -> "sync",
  "data_file_directories" -> {
    val list = new java.util.LinkedList[String]()
    list.add("/path/to/directory/on/disk1")
    list.add("/path/to/directory/on/disk2")
    list.add("/path/to/directory/on/disk3")
    list
  }
)
```
##### IMPORTANT NOTES #####
* don't use both CQL & CLI. choose only one...
* the `configMappings` key takes a sequence of `(String,java.lang.Object)`, and should be compatible with actual value represented by the key in the yaml file.
