## Spark-DFT

### Building the project

1. Modify JDK source code (to avoid a bug of phosphor)

   ```bash
   vim /usr/lib/jvm/java-1.8.0-openjdk-amd64/src.zip -> UnixLoginModule.java
   ```

   in file UnixLoginModule.java

   delete the following code

   ```java
   // This code will cause bug in phosphor
   if (ss.getGroups() != null && ss.getGroups().length > 0) {
     unixGroups = ss.getGroups();
     for (int i = 0; i < unixGroups.length; i++) {
       UnixNumericGroupPrincipal ngp =
         new UnixNumericGroupPrincipal
         (unixGroups[i], false);
       if (!ngp.getName().equals(GIDPrincipal.getName()))
         supplementaryGroups.add(ngp);
     }
   }
   ```
   
   ```
   # get the intecepted rt.jar
   wget 147.8.84.190/rt.jar
   cp rt.jar /usr/lib/jvm/java-1.8.0-openjdk-amd64/jre/lib/

   ```


2. build phosphor

   ```bash
   git clone https://github.com/jianyu-m/phosphor.git
   cd phosphor
   mvn verify
   ```

3. build spark

   ```bash
   git clone https://github.com/jianyu-m/spark.git
   ./build/mvn -DskipTests clean package
   ```





### Run the project

1. Set up the configuration file (dft.conf), a sample is

   ```ini
   # dft.conf
   dft-host = 127.0.0.1
   dft-port = 8787
   dft-phosphor-java = /home/jianyu/phosphor/Phosphor/target/jre-inst-int
   dft-phosphor-jar = /home/jianyu/phosphor/Phosphor-0.0.3-SNAPSHOT.jar
   dft-phosphor-cache = dft-cache
   dft-graph-dump-path = graph.dump
   ```

   change the phosphor_java and phosphor_jar dir to your coresponding phosphor dir

2. Start spark master and worker, then submit the task

  ```
  # submit task with tracking, need to add conf
  submit --conf spark.dft.tracking.mode=rule .....

  ```

3. stop the task and see the tracking dependency in graph.dump
