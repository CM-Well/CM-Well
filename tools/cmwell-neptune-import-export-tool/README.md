# Tool Description
- With this tool you can consume cm-well data and ingest it to an aws neptune DB.
- The tool is resumable.

# Assumption
In order to run the toold you should have
- A neptune instance
- An ec2 instance which is in the same VPC of neptune.
- An ssh tunneling between cm-well server -> ec2->neptune.
- java 8 should be used in order ro run the tool (you can use export JAVA_HOME=/opt/cm-well/app/java; export PATH=$JAVA_HOME/bin:$PATH)
- ssh version which is higher than 7.0

# How to Package the Tool
from the neptune-export-impot-tool directory(root directory) run
`sbt assembly`
Consequently, a new jar will be created under target folder

# How to Run the Tool
if you run the tool from a cm-well server you should configure ssh tunneling between cm-well server and neptune through ec2 instance.
for example: 

`ssh -i cmwell-research.pem -N -L 9999:cmwell-poc.c0zm6byrnnwp.us-east-1.neptune.amazonaws.com:8182 ec2-user@ec2-54-196-37-101.compute-1.amazonaws.com`

The tool persist the last cm-wel-position in a local directory: /tmp/cm-well
In order to mount and persist the position in an aws s3 bucket, you should install s3fs and mount /tmp/cm-well position directory.

`java -jar target/scala-2.12/<your_created_jar_name> --source-cluster "<cm-well-cluster>" --neptune-cluster "<neptune_cluster>" --ingest-threads <threads_num>`

For Example:

`java -Xmx2000m -jar target/scala-2.12/neptune-export-import-tool-assembly-0.1.jar --source-cluster "10.204.192.134:9000" --neptune-cluster "localhost:9999" --ingest-threads 500`


