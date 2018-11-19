Tool Description
With this tool you can read a large file from a server and import the data to cm-well

Assumption
The large file should be in a valid format that cm-well support such as: json, rdf, nquads etc

How to package the tool
from the importTool directory(root directory) run
sbt assembly
Consequently, a new jar will be created under target folder

How to run the tool
java -jar target/scala-2.12/<your_created_jar_name>--source-url <your_source_url> --format <your_format>

for example:
java -Xmx2000m -jar target/scala-2.12/Documents-assembly-0.1.jar --source-url "http://localhost:8080/oa-ok.ntriples" --format nquads



