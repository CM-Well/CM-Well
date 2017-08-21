# Hands-On Exercise: Add a File Infoton #

**Action:** Add an image file to CM-Well.

>**Note:** Before running the curl command, create the file: `c:\mypath\test.png`. After running the curl command, browse to \<cm-well-host\>/exercise/files/test.png and verify that the file was uploaded.

**Curl command:**

    curl -X POST <cm-well-host>/exercise/files/test.png -H "X-CM-WELL-TYPE: FILE" -H "Content-Type: image/png" --data-binary @"c:\mypath\test.png"

**Response:**

    {"success":true}

## API Reference ##
[Add File Infoton](API.Update.AddFileInfoton.md)
    
