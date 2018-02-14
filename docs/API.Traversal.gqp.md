# *gqp*: Filtering Results by Inbound and Outbound Links #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.Traversal.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Traversal.yg.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Traversal.Operands.md)  

----

## The *gqp* Operator ##

In some cases you may want to filter a group of infotons according to their inbound/outbound links and their field values, *without actually retrieving those links*.

The **gqp** flag's syntax is identical to the **yg** flag's syntax, but it operates differently. As **yg** traverses the links defined in its value expression, it adds all the infotons in the link paths to its results. **gqp** does not add infotons to the results in the set returned by the search query. Rather it attempts to evaluate its path expression for each result infoton, and if it fails at some stage in the evaluation or the expression evaluates as **false**, then the "root" infoton from which the path originated is *removed* from the result set.

The **gqp** flag can be used together with **xg** and **yg**. In this case, **gqp** takes precedence, meaning that first results are filtered by the **gqp** expression, and then expanded by **yg** and **xg**.

>**Notes:** 
>* See [Traversal Operands](API.Traversal.Operands.md) to learn about **gqp** operands.
>* The [ghost skips](API.Traversal.yg.md#hdrGhostSkips) behavior applies to **qgp** as well as **yg**.
>* The **gqp** flag can be applied to both **consume** and **search** operations. Note that when using **gqp** with consume, it's possible to filter out the entire chunk and therefore to receive no results for some iterations. If in this case you receive a 204 return code, but the position token in the header is different than the one sent, you still need to keep consuming.
>* The **total** value returned with results of a gqp-filtered query reflects the number of infotons retrieved *before* the filtering process.


## Example: Filtering Person Infotons by Address Values ##

Suppose you want to retrieve all persons of an age greater than 32, who live in New York. This condition must be applied to the following linked infotons:

<img src="./_Images/gqp-example-relation.png">

We would like to retrieve only the Person infotons, while applying filters on the linked AddressRelation and Address infotons, but *without* retrieving those linked infotons.

 Here is an example of a search clause that uses **gqp** for the scenario described above:

    <cm-well-host>/?op=search&qp=type.rdf:Person,age>32&gqp=<addressOfPerson>physicalAddress[city::New%20York]

This query only returns Person infotons for which the path defined in the **gqp** clause exists. It doesn't return the Address and AddressRelation infotons that were traversed during evaluation.

## Using the *gqp-chunk-size* Parameter ##

You can add the **gqp-chunk-size** parameter to a  **gqp** query.The **gqp-chunk-size** value determines how many infoton paths (that resulted from the query preceding the **gqp** query) will be processed at a time in a single **gqp** query. This prevents heavy **gqp** queries from "starving" other operations. 
The **gqp** query is processed in chunks of **gqp-chunk-size** until all input paths are processed.

The default value for **gqp-chunk-size** is 10. For best results, you may need to adjust the value according to the specific query you're running.


----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](API.Traversal.TOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Previous Topic**](API.Traversal.yg.md)&nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](API.Traversal.Operands.md)  

----
