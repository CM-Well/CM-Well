# Using the Curl Utility to Call CM-Well #

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](DevGuide.TOC.md
) &nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](DevGuide.BasicQueries.md
)  

----

The CM-Well API uses the REST protocol, which runs over HTTP. When running a REST GET command (one that retrieves or searches for infotons), you can simply paste the query into a browser address bar, press enter and see the query results in the browser window. 

However, PUT, POST and DELETE commands (which update CM-Well's data) cannot be submitted in a browser. There are several ways that you can submit such calls:

* From your own application, using a 3rd-party library that supports REST calls.
* Using an online utility such as [REST Test](https://resttesttest.com).
* Using a client application such as [Postman](https://www.getpostman.com).
* Using a command-line utility such as Curl.

Curl is a commonly-used utility for submitting REST requests. All the code examples that appear in CM-Well documentation use the Curl utility. 

Here is an example of a call to CM-Well, which uploads a file infoton, using Curl:

    curl -X POST <cm-well-host>/example/files/f1.png -H "X-CM-WELL-TYPE: FILE" -H "Content-Type: image/png" --data-binary @image.file.png

You can learn more about Curl from online resources such as [Using curl to automate HTTP jobs](https://curl.haxx.se/docs/httpscripting.html).

## Downloading Curl ##

You can download Curl from several sites, for example: [Curl Releases and Downloads](https://curl.haxx.se/download.html). 
Choose the package for your operating system (note that curl is already installed on Macs)

## Running Curl on Windows vs. Unix ##

If you're running Curl in a command-line window, you should be aware of some syntactical differences between the Windows and Unix command-lines:

* The line-continuation character in Unix is '\\', while in Windows it's '^'. When entering multi-line commands, remember to use the correct character for your OS.
* Unix supports single quotes as string delimiters ('string'), while Windows requires double-quotes ("string"). This can cause problems when using the JSON format, as cURL statements use single quotes to specify JSON data.

>**Note:** You can avoid many syntax errors arising from these differences by providing the data for the Curl command in a **file** rather than on the command line. Many code examples in this documentation use files as input.

You can learn more at [Using cURL in Windows](https://help.zendesk.com/hc/en-us/articles/229136847-Installing-and-using-cURL#curl_win).

----

**Go to:** &nbsp;&nbsp;&nbsp;&nbsp; [**Root TOC**](CM-Well.RootTOC.md) &nbsp;&nbsp;&nbsp;&nbsp; [**Topic TOC**](DevGuide.TOC.md
) &nbsp;&nbsp;&nbsp;&nbsp; [**Next Topic**](DevGuide.BasicQueries.md
)  

----