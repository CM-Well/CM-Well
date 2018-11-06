# Using CM-Well Downloader #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](Tools.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](Tools.UsingCM-WellDocker.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](Tools.UsingTheCM-WellIngester.md)  

----

## What is CM-Well Downloader? ##
CM-Well Downloader is a CM-Well utility for downloading infotons from CM-Well in bulk. You can run CM-Well Downloader as an alternative to calling the CM-Well streaming API from your own application. Using CM-Well Downloader is faster because its design optimizes performance for bulk downloads.

CM-Well Downloader is written in Scala and is packaged as a jar file. You can call the Downloader library from Scala or Java applications, or run the Downloader executable in a command line environment.

> **Notes:**
> * To access the CM-Well Git site, you will need a GitHub user. See [CM-Well GitHub](https://github.com/CM-Well/CM-Well) to access a CM-Well Git repository.
> * To compile and run CM-Well data tools, you will need Java version 8.
> * Graph traversal is **not** supported in streaming operations. If a streaming query contains **xg**, **yg** or **gqp** operators, they will be ignored.

## Stream-Based and Consumer-Based Downloaders ##

There are two versions of CM-Well Downloader:

* **Stream-based** - downloads a continuous stream of data. Faster than the consumer-based mode, but if it fails in the middle of downloading, you will have to download the entire stream from the start.
* **Consumer-based** - built with a Consumer design pattern. Significantly slower than the stream-based mode, but in case of failure, it can restart from the same position in the stream where it left off. For best performance, we recommend using the [bulk mode of the consumer API](API.Stream.ConsumeNextBulk.md).

Both flavors of Downloader can be found in the same library or executable file.  

## Downloading and Compiling CM-Well Source Code ##
You can download and compile the Downloader source code, to build either the stand-alone Downloader executable or the Downloader library jar.

*To install the Scala Build Tool and download the CM-Well Downloader source code:*

1. Go to [http://www.scala-sbt.org/download.html](http://www.scala-sbt.org/download.html) and install the Scala Build Tool (SBT) version appropriate for your OS.
2. Add the Scala sbt command to your PATH variable.
3. Download the CM-Well Downloader source code from [https://github.com/CM-Well/CM-Well](https://github.com/CM-Well/CM-Well).

*To build all CM-Well utility executables:*

1. Navigate to the root directory of the CM-Well data tools source code. It contains a file called **build.sbt**.
2. Run the following command: `sbt app/pack`.

The resulting shell script executables are created in the ```cmwell-data-tools-app/target/pack/bin``` folder.

*To build the CM-Well Downloader library:*

1. Navigate to the the **cmwell-downloader** directory under the data tools root directory. It contains a file called **build.sbt**.
2. Run the following command: ```sbt downloader/package```.

The resulting `cmwell-downloader_2.12-1.0.LOCAL.jar` file is created in `cmwell-data-tools/cmwell-downloader/target/scala-2.12/`.

## Running the CM-Well Downloader Executable ##

Activate the CM-Well Downloader **stream-based** executable by running: ```cmwell-data-tools-app/target/pack/bin/downloader```.

OR:

Activate the CM-Well Downloader **consumer-based** executable by running: ```cmwell-data-tools-app/target/pack/bin/consumer```.

Both applications write their output to the standard output.

Both Downloader shell scripts take parameters that mostly correspond to the CM-Well API's parameters. The following commands display the available parameters for the Downloaders:

```
cmwell-data-tools-app/target/pack/bin/downloader --help

cmwell-data-tools-app/target/pack/bin/consumer --help
```

The following table describes these parameters:

Parameter (and \<arg\>) | Description | Default Value | Relevant to
:----------------------|:-------------|:-----------------|:------------
-f, --format \<arg\> | Output format (json, jsonld, jsonldq, n3, ntriples, nquads, trig, rdfxml) | trig | Both
-h, --host \<arg\> | CM-Well server host name | N/A | Both
-l, --length \<arg\> | Maximal number of records to download (numeric value or "all" to download all records)| 50 | Both
--params \<arg\> | Parameter string to pass to CM-Well | N/A | Both
-p, --path \<arg\> | Root path to query in CM-Well  | / | Both
-q, --qp \<arg\> | Query parameters to pass to CM-Well | N/A | Both
-r, --recursive | Get data recursively under sub-paths | N/A | Both
--help | Show help message | N/A | Both
--version | Show version of this program | N/A | Both
--from-uuids | download data from uuids input stream provided by stdin | N/A | Stream
-n, --num-connections \<arg\> | The number of HTTP connections to open | 4 | Stream
-o, --op \<arg\> | Streaming type (stream, nstream, mstream, sstream). See [Streaming Data from CM-Well](DevGuide.StreamingDataFromCM-Well.md) | stream | Stream
-s, --state \<arg\> | Position state file. Provide an empty file the first time you run the command. In case of failure, the state is saved to this file, and you can then rerun the command, providing the same file, so that the operation is resumed from the previous position. | N/A | Consumer

Here is an example of how to run the stream-based CM-Well Downloader to retrieve up to 10 infotons from the PPE environment, of type Organization, containing "Thomson" in their name, under the permid.org path, with their data:

    cmwell-data-tools-app/target/pack/bin/downloader --host "cm-well-ppe.int.thomsonreuters.com" --path "/permid.org" --op stream --length 10 --qp "CommonName.mdaas:Thomson,type.rdf:Organization"

## Running CM-Well Downloader in Scala REPL Mode ##
You can run CM-Well Downloader in the Scala Interpreter (REPL), so you can examine program internals while running.

To do this, run the following command:
```
sbt downloader/console
```

After the console is successfully loaded, execute the Downloader by running the following commands:

```scala
     import cmwell.tools.data.util.akka.Implicits._
     import cmwell.tools.data.downloader.streams._
     Downloader.downloadFromQuery(
       baseUrl = "cm-well-ph",
       path = "/permid.org",
       format = "trig",
       length = Some(100),
       outputHandler = print)
 ```

 > **Note:** The outputHandler argument type is a ```String => ()``` function, which is called when new data is received. You can pass a value of ```print()``` to display on the command line, or provide a customized function (for instance, one that writes the received data to a file).

## Using the CM-Well Downloader Jar ##

For the stream-based Downloader, call the static methods of the `cmwell.tools.data.downloader.streams.Downloader` class (see the method definitions in `cmwell-downloader/src/main/scala/cmwell/tools/data/downloader/streams/Downloader.scala`).

For the consumer-based Downloader, call the static methods of the `cmwell.tools.data.downloader.consumer.Downloader` class (see the method definitions in `cmwell-downloader/src/main/scala/cmwell/tools/data/downloader/consumer/Downloader.scala`).


----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](Tools.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](Tools.UsingCM-WellDocker.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](Tools.UsingTheCM-WellIngester.md)  

----
