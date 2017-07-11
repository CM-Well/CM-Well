# Calling Custom Java/Scala Functions from SPARQL Queries #

You can write a custom function in Java or Scala, upload a Scala source file or Java/Scala jar file, and call the function from within your SPARQL query code. This allows you unlimited flexibility to manipulate the data resulting from a SPARQL query. 

Here is a comparison between the two packaging options:

| Feature                           | JAR File | Source File |
|:----------------------------------|:----------|:---------|
| Number of classes in same file    | Multiple | One |
| Languages supported               | Java/Scala | Only Scala  |
| Dependencies supported            | Yes       |  No    |
| Pre-Compilation required          | Yes        |  No      |
| File name                         | Any       |  Must match class name  |
| Can use `package` directive       | Yes    |  No |


## Step 1: Write Your Function
1. Implement a new class that extends `org.apache.jena.sparql.function.Function`, using Apache Jena 3.1.0.
1. Package it in a JAR File with any name **-OR-** just save it as `.scala` file.
1. If you have dependencies other than Jena libraries (e.g. `apache commons`), include them in your JAR.

## Step 2: Upload Your Code to CM-Well
Upload your JAR File to the `/meta/lib` CM-Well folder.
**-OR-**
Upload your Scala source file to the `/meta/lib/sources/scala` CM-Well folder.

>**Note:** Uploading a file to the `/meta/lib` folder requires a security token.

## Step 3: Call Your Function from SPARQL
1. Under the `IMPORT` section in your SPARQL query, add your JAR name and path, relative to its mandatory location (`meta/lib` or `meta/lib/sources/scala`). Make sure it ends with the correct suffix (`".jar"` or `".scala"`).
1. CM-Well registers your function as the URI `jar:<YourCanonicalFunctionName>`. You can call it either directly as a URI, or by defining a `PREFIX myJar: <jar:my.package.name.>`.

## Example ##

The following example shows how to implement a function that adds a "42_" string prefix to any value, and call this function from within a SPARQL query.

**Scala Code:**
```
package cmwell.example

import org.apache.jena.sparql.engine.binding.Binding
import org.apache.jena.sparql.expr.{ExprList, NodeValue}
import org.apache.jena.sparql.expr.nodevalue.NodeValueString
import org.apache.jena.sparql.function.FunctionEnv

import scala.collection.JavaConversions._

class Add42 extends org.apache.jena.sparql.function.Function {

  override def build(uri: String, args: ExprList): Unit = { }

  override def exec(binding: Binding, args: ExprList, uri: String, env: FunctionEnv): NodeValue = {
    val `var` = args.getList.headOption.getOrElse(new NodeValueString("arg0")).asVar()
    val res: String = `var`.asNode() match {
      case n if n.isURI => n.getURI
      case n if n.isLiteral => n.getLiteral.toString()
      case n if n.isVariable => Option(binding.get(`var`)).fold("Could not bind")(_.getLiteral.toString())
      case _ => "???"
    }
    new NodeValueString("42_" + res)
  }
}
```

Let's assume we have compiled this code using the following build configuration, and packaged it as `Add42.jar`.
```
scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
	"org.apache.jena" % "jena-arq" % "3.0.1"
)
```

The following command uploads the jar file to the correct CM-Well folder, using a valid security token:

```
curl <cm-well-host>/meta/lib/Add42.jar -H "X-CM-WELL-Type:File" \
-H "Content-Type:application/java-archive" --data-binary @Add42.jar \
-H  "X-CM-WELL-TOKEN:[a valid token]"```
```

To call the function within a SPARQL query:

```
curl cmwell/_sp --data-binary '
PATHS
/example.org/Individuals2?op=search&length=1000&with-data

IMPORT
Add42.jar

SPARQL
SELECT DISTINCT ?name ?active ?res WHERE {
?name <http://www.tr-lbd.com/bold#active> ?active .
BIND( <jar:cmwell.example.Add42>(?active) as ?res).
} ORDER BY DESC(?name)'
```


Alternatively, you could save the same Scala code as `Add42.scala`, and upload it using this command:

```
curl <cm-well-host>/meta/lib/sources/scala/Add42.scala -H "X-CM-WELL-Type:File" \
-H "Content-Type:text/plain" --data-binary @Add42.scala \
-H  "X-CM-WELL-TOKEN:[a valid token]"
```

You can then call the function as follows:

```
curl <cm-well-host>/_sp --data-binary '
PATHS
/example.org/Individuals2?op=search&length=1000&with-data

IMPORT
scala/Add42.scala

SPARQL
SELECT DISTINCT ?name ?active ?res WHERE {
?name <http://www.tr-lbd.com/bold#active> ?active .
BIND( <jar:Add42>(?active) as ?res).
} ORDER BY DESC(?name)'
```

