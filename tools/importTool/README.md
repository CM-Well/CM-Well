# Tool Description
- With this tool you can read a large file from a server and ingest its content into CM-Well.
- The tool is resumable.

# Assumption
- The large file should be in a valid (streamable) RDF format that CM-Well supports such as: NTriples, NQuads.
- It should be sorted by Subject, or at least grouped-by Subject.

# How to Package the Tool
from the importTool directory(root directory) run
`sbt assembly`
Consequently, a new jar will be created under target folder

# How to Run the Tool
`java -jar target/scala-2.12/<your_created_jar_name> --source-url <your_source_url> --format <your_format> --cluster <your_cluster> --numConn <your_number_of_connections> `

For Example:

`java -Xmx2000m -jar target/scala-2.12/Documents-assembly-0.1.jar --source-url "http://localhost:8080/oa-ok.ntriples" --format nquads --cluster localhost:9000 --numConn=3
`



