You can use the generate-password API to generate a random password and its encoded digest value. This password can be changed later if required. For example:

**Request:**

    curl <cm-well-host>/_auth?op=generate-password -H "X-CM-Well-Token:<AdminToken>"

**Response:**

    {"password":"t0OlrZGEM9","encrypted":"$2a$10$7AnXsjks.IZXTbpRiAGN4OQItwiz4sgxM49lvTiCjWgOhbbOQkg2m"}