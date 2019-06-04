# Cm-Well Docker Builds

### To build a docker image:

1. Use sbt and run packageCmwell
2. Execute the following command in this directory:
        docker build -m 4GB --build-arg useAuthorization=false -t <image-name>:latest .
### Running the image:
1. docker run -dit -p 8085:9000 --mount type=volume,destination=/home/u/app/cm-well-data,source=cm-well-data
<image-name>
        
### Get a shell to the running image:
1. docker exec -it <docker-container-hash> bash

### Stopping and restarting a container (persisting the data in cm-well)

1. docker stop <docker-container-hash>
2. docker restart <docker-container-hash>
