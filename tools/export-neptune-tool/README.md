# Tool Description
- With this tool you can consume cm-well data and ingest it to an aws neptune DB.
- The tool is resumable.
- Ingest to Neptune can be in two ways: 
    1. /sparql api - which sends sparql commands to Neptune
    2. /loader api - which allow Neptune to read content from S3 backet. This api is only for initial load(and not for updates/deletes)
- The tool supports initial load, updates and deletes of infotons

# Assumption
In order to run the tool you should have
- A neptune instance
- An ec2 instance which is in the same VPC of neptune.
- An ssh tunneling between cm-well server -> ec2->neptune.
- java 8 should be used in order ro run the tool (you can use export JAVA_HOME=/opt/cm-well/app/java; export PATH=$JAVA_HOME/bin:$PATH)
- ssh version which is higher than 7.0
- In a case you run the tool using bulk loader api, you need to allow access to aws s3 bucket.

# How to Package the Tool
from the neptune-export-impot-tool directory(root directory) run
`sbt assembly`
Consequently, a new jar will be created under target folder

# How to Run the Tool
if you run the tool from a cm-well server you should configure ssh tunneling between cm-well server and neptune through ec2 instance.
for example: 

`ssh -i cmwell-research.pem -N -L 9999:cmwell-poc.xxx.us-east-1.neptune.amazonaws.com:8182 ec2-user@ec2-54-85-12-200.compute-1.amazonaws.com`

The tool persist the last cm-wel-position, tool start time execution and update mode in a local directory: ./config.properties
In order to mount and persist the position in an aws s3 bucket, you should install s3fs and mount /tmp/cm-well position directory.

`java -jar target/scala-2.12/<your_created_jar_name> --source-cluster "<cm-well-cluster>" --neptune-cluster "<neptune_cluster>" ingest-connection-pool-size  <pool_num> --length-hint<infoton_num>`

For Example:

`java -Xmx2000m -jar target/scala-2.12/neptune-export-import-tool-assembly-0.1.jar --source-cluster "10.85.11.111:9000" --neptune-cluster "localhost:9999" --ingest-connection-pool-size  50`

- There are additional optional parameters to run the tool.
 please run `java -jar target/scala-2.12/neptune-poc-export-tool-assembly-0.1.jar --help` 
in order to see the list of parameters and their meaning
