# CM-Well Version Release Notes - Hydra (June 2017) #

## Change Summary ##


 Title | Description
:------|:-----------
yg-chunk-size parameter | You can now add a **yg-chunk-size** parameter to a  **yg** query (that searches for inbound links to a given set of paths).The **yg-chunk-size** value determines how many infoton paths (that resulted from the query preceding the **yg** query) will be processed at a time in a single **yg** query. This prevents heavy **yg** queries from "starving" other operations. The default value for **yg-chunk-size** is 10. See [Traversing Outbound and Inbound Links with xg and yg](API.TraversingOutboundAndInboundLinksWithXgAndYg.md) to learn more.
SPARQL Triggered Processor Agent | The SPARQL Triggered Processor runs SPARQL queries and constructs for the purposes of creating materialized views of CM-Well infotons. Previously, the SPARQL Triggered Processor could only be run as an external utility. Now it has an agent inside CM-Well, and its jobs are run as an integral part of CM-Well. See [Using the SPARQL Triggered Processor](Tools.UsingTheSPARQLTriggerProcessor.md) to learn more.
Processing metrics added to bg process | Various processing metrics have been added to the CM-Well bg (background) process, such as numbers and types of requests, duplicate requests and so on. Among other things, this enables analysis and suggestions for optimization of user workflows.
Log message cleanup | CM-Well log messages were reviewed and edited for clarity, conciseness and informativeness. 

### Changes to API ###
New **yg-chunk-size** parameter may be added to **yg** (inbound link) queries.


