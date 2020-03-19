# Docker image for executing the file
 
* Uses [openjdk:8-jdk-alpine](https://hub.docker.com/_/openjdk) as base image
* Installs Apache Arrow in the image
* published on Dockerhub as [fnonnenmacher/arrow-plasma-java](https://hub.docker.com/r/fnonnenmacher/arrow-plasma-java)


Commands:
* Build: `docker build -t fnonnenmacher/arrow-plasma-java:<VERSION> .`
* Push: `docker push fnonnenmacher/arrow-plasma-java:<VERSION>`