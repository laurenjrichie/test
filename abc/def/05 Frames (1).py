# Databricks notebook source
# DBTITLE 1,Load in Data
df = spark.read.json("/FileStore/tables/frames.json")
display(df)

# COMMAND ----------

# DBTITLE 1,Split turns
from pyspark.sql.functions import *

idDF = df.select("id", "labels", explode(col("turns")).alias("turn"), monotonically_increasing_id().alias("increasingID")).select("id", "increasingID", "labels.*", "turn.*")
# Monotonically increasing ID to recall ordering of turns
display(idDF)

# COMMAND ----------

# DBTITLE 1,Raw distribution of reviews
display(idDF.groupBy("userSurveyRating").count())

# COMMAND ----------

# DBTITLE 1,Bucketize Reviews (skewed)
from pyspark.ml.feature import Bucketizer

bucketizer = Bucketizer(splits=[-float("inf"), 1., 2., 3., 4., 5., float("inf")], inputCol="userSurveyRating", outputCol="rating")
bucketizedDF = bucketizer.transform(idDF.na.drop(subset="userSurveyRating"))
display(bucketizedDF.groupBy("rating").count().orderBy("rating"))

# COMMAND ----------

# DBTITLE 1,John Snow Labs Pretrained Sentiment Pipeline
from sparknlp.annotator import *
from sparknlp.common import *
from sparknlp.base import *
from pyspark.ml import Pipeline
from sparknlp.pretrained.pipeline.en import SentimentPipeline


resultDF = SentimentPipeline().pretrained().transform(bucketizedDF)

predDF = resultDF.select("id", "text", "userSurveyRating", "sentiment", when(col("sentiment")[0]["result"] == "positive", 1.).otherwise(0.).alias("prediction"))

display(predDF)

# COMMAND ----------

# MAGIC %md John Snow Labs Pretrained model doesn't do well with "neutral" text. For example,  `I do not have any dates in mind. I would like to spend as much time in Denver as my budget will allow.` is labelled as negative

# COMMAND ----------

# DBTITLE 1,NLTK 
import nltk
from nltk.sentiment.vader import *
from pyspark.sql.types import *

@udf(MapType(StringType(), DoubleType()))
def sentimentAnalyzer(text):
  nltk.download('vader_lexicon')
  sia = SentimentIntensityAnalyzer()
  return sia.polarity_scores(text)

# COMMAND ----------

nltkDF = bucketizedDF.withColumn("nltkPred", sentimentAnalyzer(col("text")))
display(nltkDF.select("id", "text", "nltkPred", "userSurveyRating"))

# COMMAND ----------

# DBTITLE 1,Analyze Pos/Neg/Neutral Distribution
display(nltkDF.select(sum(col("nltkPred.neg")).alias("neg"), sum(col("nltkPred.pos")).alias("pos"), sum(col("nltkPred.neu")).alias("neu")))

# COMMAND ----------

# DBTITLE 1,Verify with Pre-labelled data
labelDF = spark.read.parquet("tmp/brooke/imdb_amazon_yelp.parquet")

labelPredDF = labelDF.withColumn("nltkPred", sentimentAnalyzer(col("text"))).select("*", "nltkPred.neg", "nltkPred.pos", "nltkPred.neu", "nltkPred.compound")
display(labelPredDF)

# COMMAND ----------

# DBTITLE 1,Positive > Neg
resDF = labelPredDF.select("*", when(col("pos") > col("neg"), 1.).otherwise(0.).alias("prediction"))

display(resDF)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Results
# MAGIC 
# MAGIC Neutral has a very strong weight in many of these reviews.
# MAGIC 
# MAGIC `AN HOUR... seriously?` is .459 negative, .541 neutral
# MAGIC 
# MAGIC `the staff is friendly and the joint is always clean.` .424 pos, .576 neutral
# MAGIC 
# MAGIC Mistakes:
# MAGIC 
# MAGIC `Phone is sturdy as all nokia bar phones are.`, `Works great!.` and `Very wind-resistant.` are positive, but prediction is entirely neutral.
# MAGIC 
# MAGIC `Cumbersome design.` and `I don't think it would hold it too securly on your belt.` are negative, but prediction is entirely neutral.
# MAGIC 
# MAGIC `Never been to Hard Rock Casino before, WILL NEVER EVER STEP FORWARD IN IT AGAIN!` is flagged as slightly positive.

# COMMAND ----------

# DBTITLE 1,Isotonic Regression Model
from pyspark.ml.feature import RFormula
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.regression import IsotonicRegression
from pyspark.ml.pipeline import Pipeline

labelPredDF1 = labelPredDF.withColumn("proportion", (col("pos"))/(col("neg") + col("neu")/3 + .000000001)) # When include neu in numerator, dominates it
trainDF, testDF = labelPredDF1.randomSplit([.8, .2], seed=42)

rf = RFormula(formula="label ~ neg + pos + neu + compound")
lr = LogisticRegression(fitIntercept=True)
ir = IsotonicRegression(featuresCol='proportion', predictionCol='prediction', isotonic=True)
pipeline = Pipeline(stages=[ir])

pipelineModel = pipeline.fit(trainDF)
testPredDF = pipelineModel.transform(testDF)

# COMMAND ----------

display(testPredDF)

# COMMAND ----------

# DBTITLE 1,Logistic Regression Model
from pyspark.ml.feature import RFormula
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.regression import IsotonicRegression
from pyspark.ml.pipeline import Pipeline

trainDF, testDF = labelPredDF.randomSplit([.8, .2], seed=42)

rf = RFormula(formula="label ~ neg + pos + neu + compound")
lr = LogisticRegression(fitIntercept=True)
pipeline = Pipeline(stages=[rf, lr])

pipelineModel = pipeline.fit(trainDF)
testPredDF = pipelineModel.transform(testDF)

lrModel = pipelineModel.stages[1]
print("Intercept: ", lrModel.intercept)
list(zip(["neg", "pos", "neu", "compound"], lrModel.coefficients))

# COMMAND ----------

display(testPredDF)

# COMMAND ----------

display(testPredDF.orderBy("probability"))

# COMMAND ----------

display(testPredDF.filter("label != prediction"))

# COMMAND ----------

# DBTITLE 1,Confusion Matrix (more False Negatives)
display(testPredDF.groupBy("label", "prediction").count()) 

# COMMAND ----------

# DBTITLE 1,Evaluate
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

evaluator = MulticlassClassificationEvaluator(predictionCol='prediction', labelCol='label', metricName='accuracy')

metricsDF = spark.createDataFrame([("f1", evaluator.evaluate(testPredDF)), 
                                   ("accuracy", evaluator.setMetricName("accuracy").evaluate(testPredDF))], ["Metric", "Value"])
display(metricsDF)

# COMMAND ----------



# COMMAND ----------

