# The Protocol System Field #

When you ingest an infoton whose subject URL is ```<protocol>/<path>```, the ```<protocol>``` value is stored in the **protocol** system field, while the ```<path>``` value is stored in the **path** system field.

For example, when an infoton whose URL is ```https://geo/countries``` is ingested, it will have the following system field values:

    protocol="https"
    path="/geo/countries"

The ingested infoton can have either **http** or **https** as its protocol value. If no protocol value is supplied, a default value of **http** is assumed.

>**Notes**: 
>* This feature is not related to the protocol used to access CM-Well, which can be either **http** or **https**. Regardless of the protocol used to access CM-Well, ingested infotons can have either protocol in their URL.
>* For a triple added to CM-Well, there is only special handling of the protocol in the Subject URL. A URL in an Object is stored as-is, complete with the protocol prefix, if provided.
>* The **protocol** system field is retrieved when an infoton is read via API, in any format. However, is it not displayed in the CM-Well Web UI.
>* If an infoton was ingested before the **protocol** system field was introduced, a default value of **http** will be retrieved when that infoton is read.
>* Two infotons that are identical except for their protocol are still considered identical. For example, if infoton A is written with the **http** protocol, and then updated with the same path but with the **https** protocol, then the original instance of A will be updated. However, if during an ingest, nothing is changed but the protocol value, this is not considered a null update - the **protocol** value will be updated and the **lastModified** time will be updated.


 

 

 

 
