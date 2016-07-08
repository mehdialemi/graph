#   -----------------------------------------------------
#               My Spark Default Configuration
#   -----------------------------------------------------

spark.master                     spark://localhost:7077
#spark.dynamicAllocation.enabled         true
#spark.shuffle.service.enabled   true
spark.driver.memory              1g
spark.executor.memory            1g
spark.memory.fraction            0.75
spark.memory.storageFraction     0.5
spark.eventLog.enabled           true
spark.eventLog.dir               /home/mehdi/spark-event-log
spark.local.dir                 /tmp
spark.serializer                org.apache.spark.serializer.KryoSerializer
spark.akka.frameSize            128
spark.shuffle.compress          true
spark.shuffle.file.buffer       32k

#spark.shuffle.io.numConnectionsPerPeer Increase this conf if you have more than one disk per machine
spark.shuffle.io.numConnectionsPerPeer  1

spark.io.compression.codec              lz4
#spark.kryo.registrationRequired                true
spark.rdd.compress              false
spark.kryoserializer.buffer     64m
spark.executor.extraJavaOptions -XX:+UseParallelGC -XX:+UseParallelOldGC -XX:ParallelGCThreads=3 -XX:MaxGCPauseMillis=100 -XX:+UseCompressedOops -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xmn200m

# Add -XX:+UseCompressedOops to GC options if you have less than 32G Ram per worker
# By considering GC log:
#   - OldGen is close to full => lower spark.memory.storageFraction
#   - full GC => There isn't enough memory for executing of tasks.
#   - Too many minor collection => Increase eden (-Xmn=...). For example if you have 4 tasks each read a block of HDFS
#     (with 64M) then your eden size should be at least 4 * 3 * 64. Here, 3 stands for size of block after
#     decompressing.
