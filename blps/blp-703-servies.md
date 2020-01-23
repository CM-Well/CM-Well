# Hosting Services Exposed by Routes

- CM-Well will host services and expose configurable endpoints to access them
- Services might be internal built-in ones, or external ones, ingested
- Managing services will take place declerativly by ingesting/modyfing/deleting Infotons in `/meta/services/`
- Each ServiceInfoton will have a type, and configuration as fields
- `route` will be a mandatory field, and for simplicty will simply test for `path.startsWith`
- WS will consume `/meta/services` from time to time, and dynamically change GET logic accordingly
- This serves our cloud-friendliness goal, we can have one recipe of uploading a JAR or a source file, and start using it. Regardless of underlying infrastructure

## Types
- `Redirection` would be the most trivial one
    - it will require a regex based replacement to the service endpoint
- Down the road we will come up with more types, e.g. `Source` or `Binary`
- Most of types will reqire the `bin` or `src` properties with the value of the logic Infoton's path

## Examples

### First example: Redirection

```
/meta/services/company

type.rdf:		cmwell://meta/sys#Redirection
route:			/company/
regex:			/company/$1
replacement:	/permid.org/1-$1
```

- This will redirect any GET request to /company/Something to /permid.org/1-Something
- for example /company/8589934184 will redirect to /permid.org/1-8589934184

### Second example: External Service

```
/meta/src/echo-service (FileInfoton)

def main(args: Array[String]) = s"<html><u>${args.head}</u></html>"
```

```
/meta/services/echo

type.rdf:		cmwell://meta/sys#Source
route:			/echo/
src:			/meta/src/echo-service
args:			/msg/$1
returnType:		FileInfoton
mimeType:		text/html
```

Having those two Infotons can enable something like that:

Requesting `/echo/Hello%20World` will return: <u>Hello World</u>

## Service State
As a first phase we can assume each service is stateless, and is pure functional with no side effects but just a main-like function that accept arguments and return a value. Down the road we can make those in SPI/Plugins manner, e.g. implement a known trait which have a handle WS can invoke, but the service itself will kept up and running and can maintain its own state. This is very much like the difference between CGI and FastCGI.

## Service Instances
A service (not an inlined one such as Redirection) will have two modes of definition: A Singleton (exactly one in a cluster (easy to impl. using Grid's SingletonActor)), or on each CM-Well node
