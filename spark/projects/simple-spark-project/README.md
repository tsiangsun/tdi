To compile and package up the jar (there may be further instructions if this is your first sbt project):
```bash
sbt package
```

To run the jar locally:
```bash
$SPARK_HOME/bin/spark-submit \
  --class "com.thedataincubator.simplespark.SimpleApp" \
  --master local[4] \
  target/scala-2.10/simple-project_2.10-1.0.jar
```

At the end of the job, you'll notice we can't see the printed output 
you would expect from looking at the source files. Here's how to view that:

1. `sudo su` to root user, and `cd /var/log/hadoop-yarn/userlogs`. 
You'll see one or more directories corresponding to jobs you've run. Change into the directory whose
ID matches the ID of the job you just ran (the ID was output many times during your job run, it's also
generally the largest numerical run).

2. Look at the `stdout` file inside of each of the `container_...` subdirectories. One will contain the 
expected data. 

3. In general, you should specify a file on HDFS that you want your output to go to, and then look at that file. Additionally, make note of this 
directory, as looking through the `stdout` and `stderr` files allows you to debug jobs you have running more easily.

You can also run commands inside this environment
```bash
sbt console
```
and obtain an interactive scala prompt inside your jar
```scala
scala> val x = 1
x: Int = 1

scala> import com.thedataincubator.simplespark.SimpleApp
import com.thedataincubator.simplespark.SimpleApp


```
