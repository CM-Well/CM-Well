var Uploader = function(form) {
    this.form = form;
};

Uploader.prototype = {
    headers : {},

//    /**
//     * @return Array
//     */
//    get elements() {
//        var fields = [];
//
//        // gather INPUT elements
//        var inputs = this.form.getElementsByTagName("INPUT");
//        for (var l=inputs.length, i=0; i<l; i++) {
//            fields.push(inputs[i]);
//        }
//
//        // gather SELECT elements
//        var selects = this.form.getElementsByTagName("SELECT");
//        for (var l=selects.length, i=0; i<l; i++) {
//            fields.push(selects[i]);
//        }
//
//        return fields;
//    },


    /**
     * @return String
     */
//    generateBoundary : function() {
//        return "---------------------------" + (new Date).getTime();
//    },

    /**
     * @param  Array elements
     * @param  String boundary
     * @return String
     */
    buildMessage : function(elements, boundary) {
        var CRLF  = "\r\n";
        var parts = [];

        elements.forEach(function(element, index, all) {
            var part = "";
            var type = "TEXT";

            if (element.nodeName.toUpperCase() === "INPUT") {
                type = element.getAttribute("type").toUpperCase();
            }

            if (type === "FILE" && element.files.length > 0) {
                var fieldName = element.name;
                var fileName  = element.files[0].fileName;

                /*
                 * Content-Disposition header contains name of the field used
                 * to upload the file and also the name of the file as it was
                 * on the user's computer.
                 */
                part += 'Content-Disposition: form-data; ';
                part += 'name="' + fieldName + '"; ';
                part += 'filename="'+ fileName + '"' + CRLF;

                /*
                 * Content-Type header contains the mime-type of the file to
                 * send. Although we could build a map of mime-types that match
                 * certain file extensions, we'll take the easy approach and
                 * send a general binary header: application/octet-stream.
                 */
                part += "Content-Type: application/octet-stream" + CRLF + CRLF;

                /*
                 * File contents read as binary data, obviously
                 */
                part += element.files[0].getAsBinary() + CRLF;
            } else {
                /*
                 * In case of non-files fields, Content-Disposition contains
                 * only the name of the field holding the data.
                 */
                part += 'Content-Disposition: form-data; ';
                part += 'name="' + element.name + '"' + CRLF + CRLF;

                /*
                 * Field value
                 */
                part += element.value + CRLF;
            }

            parts.push(part);
        });

        var request = "--" + boundary + CRLF;
            request+= parts.join("--" + boundary + CRLF);
            request+= "--" + boundary + "--" + CRLF;

        return request;
    },

    /**
     * @return null
     */
    send : function() {
        var boundary = this.generateBoundary();
        var xhr      = new XMLHttpRequest;

        xhr.open("POST", this.form.action, true);
        xhr.onreadystatechange = function() {
            if (xhr.readyState === 4) {
                alert(xhr.responseText);
            }
        };
        var contentType = "multipart/form-data; boundary=" + boundary;
        xhr.setRequestHeader("Content-Type", contentType);

        for (var header in this.headers) {
            xhr.setRequestHeader(header, headers[header]);
        }

        // finally send the request as binary data
        xhr.sendAsBinary(this.buildMessage(this.elements, boundary));
    }
};
