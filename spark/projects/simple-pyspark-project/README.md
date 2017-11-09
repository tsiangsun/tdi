This is a simple pyspark app that counts the number of lines that contain an `a` and the number of lines that contain a `b`.  The main application is in SimpleApp.py, with a utility function broken out into myutils.py.  Run it locally with
```bash
spark-submit --master local[*] --py-files myutils.py SimpleApp.py <HDFS path>
```

**Instructors:**
These files should already be on the `s3://dataincubator-spark-demo/` bucket.  Create  a small cluster with
```bash
aws emr create-cluster --release-label emr-5.4.0 --name 'Spark' --log-uri 's3://dataincubator-spark-demo/logs' --use-default-roles --ec2-attributes KeyName=spark-demo --applications Name=Spark --instance-type m3.xlarge --instance-count 3
```

And then run it on a bunch of the Stack Overflow data with
```bash
aws emr add-steps --cluster-id <id> --steps Name=SimpleApp,Type=spark,Args=[--deploy-mode,cluster,--master,yarn,--py-files,s3://dataincubator-spark-demo/src/myutils.py,s3://dataincubator-spark-demo/src/SimpleApp.py,s3://dataincubator-course/spark-stats-data/allPosts/],ActionOnFailure=CONTINUE
```

Don't forget to shut down the cluster afterwards
```bash
aws emr terminate-clusters --cluster-id <id>
```
