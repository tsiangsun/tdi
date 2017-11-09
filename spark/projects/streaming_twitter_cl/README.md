## Build project:
```bash
sbt assembly
```

## Collect Tweets

```bash
spark-submit \
--class "twitterclassifier.Collect" \
--master local[*] \
target/scala-2.10/spark-twitter-lang-classifier-assembly-1.0.jar ~/datacourse/secrets/twitter_secrets.json.nogit tmptweets 1000 10 1
```

#### ExamineAndTrain.scala
```bash
spark-submit \
--class "twitterclassifier.ExamineAndTrain" \
--master local[*] \
target/scala-2.10/spark-twitter-lang-classifier-assembly-1.0.jar "tmptweets/tweets_*/*" model 3 10
```

#### Predict.scala
```bash
spark-submit \
--class "twitterclassifier.Predict" \
--master local[*] \
target/scala-2.10/spark-twitter-lang-classifier-assembly-1.0.jar ~/datacourse/secrets/twitter_secrets.json.nogit model 2 5
```
