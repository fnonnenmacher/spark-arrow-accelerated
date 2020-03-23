# Docker image for executing the file
 
* Uses [alpine:3.11](https://hub.docker.com/_/alpine) as base image
* Installs Apache Arrow in the image with the JNI plasma library
* Installs fletcher runtime and fletcher platform echo
* published on Dockerhub as [fnonnenmacher/arrow-plasma-java](https://hub.docker.com/r/fnonnenmacher/arrow-plasma-java)

## Version
* `0.1`: Contains jdk-8, apache arrow
* `0.2`: Adds fletcher runtime and echo platform 

## Commands:
* Build: `docker build -t fnonnenmacher/arrow-plasma-java:<VERSION> .`
* Push: `docker push fnonnenmacher/arrow-plasma-java:<VERSION>`