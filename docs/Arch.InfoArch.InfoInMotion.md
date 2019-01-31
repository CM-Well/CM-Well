# Information Architecture Part 3: Information in Motion

Unlike standard triple-stores that focus on reading relatively static data, CM-Well is specifically designed to handle data that is constantly updated. We use the term "information in motion" to describe the features of dynamic information flow that CM-Well is designed to support.

This includes features such as:

- Convenient batch uploading, including processing commands expressed within the triples input, such as replacement or deletion of triples. This allows the user to perform relatively complex operations without the use of complex tools such as SPARQL commands.
- Virtual queues (user defined subsets ordered by time, that can be consumed by traversing the timeline with a token that encodes the position in the queue)
- Convenient batch processing of information pulled from CM-Well, either a full or partial copy of CM-Well's data. This includes various streaming and subscription features.
- SPARQL Trigger Processor (STP) – an automatic agent that identifies changes in pre-defined groups of infotons and applies SPARQL processing to those groups. Used for creating materialized views or deriving inferred data.
- Managing historical versions using immutable data principles. Updates are implemented by adding new layers of information.
- Subscribing to real-time updates
- Transparent scaling mechanisms
- Support of "edge devices" – small CM-Well clusters that contain a subset of CM-Well data.

The following image illustrates CM-Well's "Not Only Copy" workflows:

<img src="./\_Images/no-copy.png" align="middle">