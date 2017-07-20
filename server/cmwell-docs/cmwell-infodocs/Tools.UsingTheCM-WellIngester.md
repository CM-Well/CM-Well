# Using the CM-Well Ingester #

## What is the CM-Well Ingester? ##

The CM-Well Ingester is a CM-Well utility for uploading infotons to CM-Well in bulk. Using the Ingester, you can upload data from a file, the standard input, or a customized input source. The Ingester processes several upload requests in parallel, up to a configurable limit.

The CM-Well Ingester is written in Scala and is packaged as a jar file (Java library). You can call it from a Java or Scala application, or in a command-line environment.

## Downloading and Compiling CM-Well Source Code ##

> **Notes:** 
> * To access the CM-Well Git site, you will need a GitHub user. See the [CM-Well GitHub Repository](https://github.com/thomsonreuters/CM-Well).
> * To compile and run CM-Well data tools, you will need Java version 8.

*To install the Scala Build Tool and download the CM-Well Ingester source code:*

1. Go to [http://www.scala-sbt.org/download.html](http://www.scala-sbt.org/download.html) and install the Scala Build Tool (SBT) version appropriate for your OS.
2. Add the Scala sbt command to your PATH variable.
3. Download the CM-Well Downloader source code from [https://github.com/thomsonreuters/CM-Well](https://github.com/thomsonreuters/CM-Well).

*To build all CM-Well utility executables:*

1. Navigate to the root directory of the CM-Well tools source code. It contains a file called **build.sbt**.
2. Run the following command: ```sbt app/pack```.

The resulting shell script executables are created in ```cmwell-data-tools-app/target/pack/bin``` folder.

*To build only the CM-Well Ingester library:*

1. Navigate to the the **cmwell-ingester** directory under the tools root directory. It contains a file called **build.sbt**.
2. Run the following command: ```sbt ingester/package```. 

The resulting `cmwell-ingester_2.11-1.0.LOCAL.jar` file is created in `cmwell-data-tools/cmwell-ingester/target/scala-2.11/`.

## Running the CM-Well Ingester as an Executable ##

To run the CM-Well Ingester as a stand-alone executable, run the following command:
```
cm-well-ingester/target/pack/bin/ingester --host <HOST> --format <FORMAT>
```
The following table describes the input parameters:

Parameter | Description
:---------|:-------------
-b, --backpressure  <arg> | Maximal number of parallel upload requests (default = 100)                            
-f, --format  <arg> | Format of uploaded data (e.g. ntriples, nquads, jsonld...)
-h, --host  <arg> | CM-Well host name
-p, --port  <arg> | CM-Well HTTP host port (default = 80)
--help  | Show the usage description
--version  | Show the version number of this program

>**Notes:**
>* To display a description of the ingester parameters, run `ingester --help`. 
>* Usually you will not have to change the **backpressure** value. But this may become advisable if, for example, you're receiving "busy" messages from CM-Well.
>* See [Ingesting Each Infoton as an Atomic Operation](#hdrAtomic).

Here is an example of how to run the CM-Well Ingester as a stand-alone executable, while taking the input data from a file:
```
cmwell-data-tools-app/target/pack/bin/ingester --host "cm-well-ph" --format ntriples --file input_file.nt
```

## Using the CM-Well Ingester Jar ##

To use the Ingester class, you will need to add the following import statements to your code:

    import cmwell-tools.data.ingester.Ingester.{IngestFailEvent, IngestSuccessEvent}
    import cmwell-tools.data.utils.ArgsManipulations._
    import cmwell-tools.data.utils.akka._
    import Implicits._

To ingest data, call the static methods of the **cmwell.tools.data.ingester.Ingester** class. You can call  **Ingester.ingest(...)**, **Ingester.fromInputStream(...)** or **Ingester.fromPipe(...)**, depending on the input object you prefer. 

See the method definitions in **cmwell-downloader/src/main/scala/cmwell/tools/data/ingester/Ingester.scala**.

>**Note:** See [Ingesting Each Infoton as an Atomic Operation](#hdrAtomic).

<a name="hdrAtomic"></a>
## Ingesting Each Infoton as an Atomic Operation ##

Each infoton that you add to CM-Well must be ingested in one atomic operation.
To ensure this, you must provide all of the infoton's data, including all field values, in a single line of input, terminated by the \n character.

When calling the Ingester jar, you can use the utility class **GroupChunker** to group infoton data in a single line, using an infoton separator string that you define.

## Running CM-Well Ingester in Scala REPL Mode ##

You can run CM-Well Ingester in the Scala Interpreter (REPL), so you can examine program internals while running.

To do this, run the following command:
```
sbt ingester/console
```

After the console is successfully loaded, the following commands read input from a file and upload it to CM-Well:
```scala
import cmwell.tools.data.util.akka.Implicits._
import cmwell.tools.data.ingester.streams._
import java.io._

Ingester.fromInputStream(
  host = "cm-well-ph",
  format = "ntriples",
  in = new FileInputStream("input.nt"))
```

The following commands do the same with an InputStream source:
```scala
val is = new ByteArrayInputStream("ntriples data...".getBytes("utf8"))
Ingester.fromInputStream(
  host = "cm-well-ph",
  format = "ntriples",
  in = is)
```
Remember that each infoton's data must be contained within a single line. For example, if an infoton has multiple lines of ntriples data, the string would look like this:
```
val data = """
| <http://example.org/subject-1> <http://example.org/predicate#p1> "1"^^<http://www.w3.org/2001/XMLSchema#int> .
| <http://example.org/subject-1> <http://example.org/predicate#p2> "2"^^<http://www.w3.org/2001/XMLSchema#int> .
| <http://example.org/subject-1> <http://example.org/predicate#p3> "3"^^<http://www.w3.org/2001/XMLSchema#int> .
|"""
```
Here is an example of JSON-LD formatted data in a single string:
```JSON
    val data = """
    {   "@id" : "o:PeterParker",   "dataCenter" : "ph",   "indexTime" : "1447839602260",  
    "lastModified" : "2015-11-18T09:40:01.126Z",   "parent" : "/example.org/Individuals",  
    "path" : "/example.org/Individuals/PeterParker",   "type" : "ObjectInfoton",  
     "uuid" : "fae99c4442f0dced28a3e6d50159c7c2",   "neighborOf" : "o:ClarkKent" }
    """
```
## Implementing Custom Input Sources ##

Instead of providing input from a file or from `stdin`, you can implement custom input sources (e.g. JDBC, socket...). To do so, you can either use one of the `akka.stream.scaladsl.Source` methods to create a new custom source, or use one of the methods already provided by Akka within `akka.stream.scaladsl.StreamConverters`.

For example: the method `Source.fromPublisher(Props(classOf[CustomActor[T]])` creates a custom source from an actor publisher. Another option is to use `source = StreamConverters.fromInputStream(() => in)` in order to create an input source from an InputStream, referenced by the `in` variable.

When implementing a custom input source, it's up to the developer to group together data lines which belong to the same infoton (e.g., group together triples that belong to the same infoton).

The following lines show how you can create an input source from a given file:
```scala
import cmwell.tools.data.util.akka._
val source = StreamConverters.fromInputStream(new FileInputStream(file))
  .via(Framing.delimiter(endl, maximumFrameLength = Int.MaxValue, allowTruncation = true))
  .via(GroupChunker(GroupChunker.formatToGroupExtractor("ntriples")))
  .map(concatByteStrings(_, endl)) // merge lines to a single element
  .map(_.utf8String)
```
Data chunks are split according to the `\n` character. Each frame is converted to a UTF-8 encoded string. Lines related to the same infoton must be grouped together in a single string. You can use the `cmwell.tools.data.util.chunkers.GroupChunker` class for this purpose. This class's constructor receives a function for grouping the lines. In this case, lines are grouped by having the same subject.


