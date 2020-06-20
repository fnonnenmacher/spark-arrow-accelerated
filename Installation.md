# Installation of Preconditions

This guide focuses on installing the preconditions on a system without admin rights. Therefore, this guide avoids the usage of packet managers and installs all software into a custom directory.

With this conditions this description is suitable for the [servers of the research group](http://qce-it-infra.ewi.tudelft.nl/qce_servers.html). When you install it on Power9 pay attention to the special hints!

## Directories

We use the directory `$WORK` to download all required projects and install all software binaries into `$WORK/local`. To include the installed files we can directly update the path environment variables:
```
export PATH=$WORK/local/bin:$PATH
export LD_LIBRARY_PATH=$WORK/local/lib:$WORK/local/lib64:$PATH
```
  
## Protobuf
> Make sure to use the same version as used by [apache arrow](https://github.com/apache/arrow/blob/apache-arrow-0.17.1/cpp/thirdparty/versions.txt#L43) to avoid conflicts

```
cd $WORK
wget -q https://github.com/protocolbuffers/protobuf/releases/download/v3.7.1/protobuf-cpp-3.7.1.tar.gz
tar -xzf protobuf-cpp-3.7.1.tar.gz
cd protobuf-3.7.1/
./configure --prefix=$WORK/local
make
make install
ldconfig
```

## CMake
The latest cmake source code can be found [here](https://cmake.org/download/).

```
wget https://github.com/Kitware/CMake/releases/download/v3.17.3/cmake-3.17.3.tar.gz
tar -xzf cmake-3.17.3.tar.gz
mkdir cmake-3.17.3/build
cd cmake-3.17.3/build
cmake -DCMAKE_INSTALL_PREFIX=$WORK/local ..
make
make install
```

## LLVM

For installing LLVM we follow [this tutorial](https://xuechendi.github.io/2019/07/12/Apache-Arrow-Gandiva-on-LLVM).

```
cd $WORK
wget https://github.com/llvm/llvm-project/releases/download/llvmorg-8.0.1/llvm-8.0.1.src.tar.xz
tar xf llvm-8.0.1.src.tar.xz
cd llvm-8.0.1.src/tools/
wget https://github.com/llvm/llvm-project/releases/download/llvmorg-8.0.1/cfe-8.0.1.src.tar.xz
tar xf cfe-8.0.1.src.tar.xz
mv cfe-8.0.1.src clang
mkdir ../build
cd ../build
cmake -DLLVM_HAS_RTTI=1 -DLLVM_ENABLE_RTTI=ON -DCMAKE_INSTALL_PREFIX=$WORK/local ..
make -j <NUMBER OF CORES>
make install
```

## Java and Maven

For installing java and java tooling the easiest way is [sdkman](https://sdkman.io)

```
export SDKMAN_DIR="$WORK/sdkman" && curl -s "https://get.sdkman.io" | bash
source "$WORK/sdkman/bin/sdkman-init.sh"
sdk install java 8.0.252.hs-adpt # you can can check with "sdk list java" for available versions
export JAVA_HOME=$WORK/sdkman/candidates/java/current
sdk install maven
```

Java cannot be installed with sdkman on Power Systems, therefore get the file [here](https://developer.ibm.com/javasdk/downloads/sdk8/)

```
wget http://public.dhe.ibm.com/ibmdl/export/pub/systems/cloud/runtimes/java/8.0.6.10/linux/ppc64le/ibm-java-sdk-8.0-6.10-ppc64le-archive.bin
chmod +x ibm-java-sdk-8.0-6.10-ppc64le-archive.bin
./ibm-java-sdk-8.0-6.10-ppc64le-archive.bin #asks you for install dir e.g. $WORK/$ibm-java-ppc64le-80
# Register anyway in sdkman (easy for using different versions and settign up env variables e.g. JAVA_HOME)
sdk install java ibm-power-sys $WORK/ibm-java-ppc64le-80/
export JAVA_HOME=$WORK/sdkman/candidates/java/current
```

## Apache Arrow including Gandiva jar
```
cd $WORK
wget https://github.com/apache/arrow/archive/apache-arrow-0.17.1.tar.gz
tar -xzf apache-arrow-0.17.1.tar.gz
mkdir arrow-apache-arrow-0.17.1/cpp/build
cd arrow-apache-arrow-0.17.1/cpp/build
cmake -DCMAKE_INSTALL_PREFIX=$WORK/local -DARROW_PARQUET=ON -DARROW_GANDIVA=ON -DARROW_GANDIVA_JAVA=ON -DARROW_DATASET=ON -DARROW_WITH_SNAPPY=ON ..
make
make install
```

Hint: Verify cmake has found the correct protobuf version. The cmake standard output should look similar to this:
```
[...]
-- Found protoc: $WORK/local/bin/protoc
-- Found libprotoc: $WORK/local/lib/libprotoc.so
-- Found libprotobuf: $WORK/local/lib/libprotobuf.so
-- Found protobuf headers: $WORK/local/include
[...]
```

Building the Gandiva jar file
```
cd $WORK/arrow-apache-arrow-0.17.1/java
export MAVEN_OPTS="-Xmx4096m -XX:MaxPermSize=512m" # When you need more memory
mvn install -pl gandiva -P arrow-jni -am -DskipTests -Darrow.cpp.build.dir=../../cpp/build/release
```

> **Hint** for Linux Power Systems

Building the gandiva jar is a bit tricky, because the currently used dependencies are not compatible with it. Two things to do

* Increase the protobuf version in `...java/gandiva/pom` to 3.7.1
```
<protobuf.version>3.7.1</protobuf.version>
``` 
* Build Flatbuffers in version v1.10.0 and install it as v1.9.0 to maven local
```
cd $WORK
wget https://github.com/google/flatbuffers/archive/v1.10.0.tar.gz
tar -xzf v1.10.0.tar.gz
mkdir flatbuffers-1.10.0/build
cd flatbuffers-1.10.0/build
cmake ..
make
# Go to the Gandiva project
cd $WORK/arrow-apache-arrow-0.17.1/java
mvn install:install-file -DgroupId=com.github.icexelloss -DartifactId=flatc-linux-ppcle_64 -Dversion=1.9.0 -Dpackaging=exe -Dfile=$WORK/flatbuffers-1.10.0/build/flatc
```

## Fletcher
```
git clone https://github.com/abs-tudelft/fletcher.git # We use the latest dev version, check that it is compatible with the apache arrow
mkdir fletcher/runtime/cpp/build
cd fletcher/runtime/cpp/build
cmake -DCMAKE_INSTALL_PREFIX=$WORK/local ..
make
make install
mkdir fletcher/platforms/echo/runtime/build
cd fletcher/platforms/echo/runtime/build
cmake -DCMAKE_INSTALL_PREFIX=$WORK/local ..
make
make install
```

TODO: Fletcher runtime e.g. echo plattform