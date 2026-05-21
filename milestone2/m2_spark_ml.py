import sys
import time

from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, lower, to_timestamp, when


FEATURE_NAMES = ["District", "crime_index", "Hour", "domestic_index"]


def prepare_data(spark, data_path):
    raw_df = spark.read.csv(data_path, header=True, inferSchema=True)
    return (
        raw_df.withColumn("Hour", hour(to_timestamp(col("Date"), "MM/dd/yyyy hh:mm:ss a")))
        .withColumn("District", col("District").cast("double"))
        .withColumn("Domestic_str", lower(col("Domestic").cast("string")))
        .withColumn("label", when(lower(col("Arrest").cast("string")) == "true", 1).otherwise(0))
        .select("District", "Primary Type", "Hour", "Domestic_str", "label")
        .dropna()
    )


def feature_stages():
    crime_indexer = StringIndexer(
        inputCol="Primary Type",
        outputCol="crime_index",
        handleInvalid="skip",
    )
    domestic_indexer = StringIndexer(
        inputCol="Domestic_str",
        outputCol="domestic_index",
        handleInvalid="skip",
    )
    assembler = VectorAssembler(inputCols=FEATURE_NAMES, outputCol="features")
    return [crime_indexer, domestic_indexer, assembler]


def confusion_matrix(predictions):
    rows = predictions.groupBy("label", "prediction").count().collect()
    values = {(int(r["label"]), int(r["prediction"])): r["count"] for r in rows}
    return (
        values.get((0, 0), 0),
        values.get((0, 1), 0),
        values.get((1, 0), 0),
        values.get((1, 1), 0),
    )


def train_and_score(name, classifier, stages, train_df, test_df):
    pipeline = Pipeline(stages=stages + [classifier])
    start = time.time()
    model = pipeline.fit(train_df)
    train_time = time.time() - start
    predictions = model.transform(test_df)

    binary_eval = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC",
    )
    multi_eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
    tn, fp, fn, tp = confusion_matrix(predictions)

    return model, {
        "Model": name,
        "AUC": binary_eval.evaluate(predictions),
        "Accuracy": multi_eval.evaluate(predictions, {multi_eval.metricName: "accuracy"}),
        "F1": multi_eval.evaluate(predictions, {multi_eval.metricName: "f1"}),
        "Precision": multi_eval.evaluate(predictions, {multi_eval.metricName: "weightedPrecision"}),
        "Recall": multi_eval.evaluate(predictions, {multi_eval.metricName: "weightedRecall"}),
        "Training Time": train_time,
        "TN": tn,
        "FP": fp,
        "FN": fn,
        "TP": tp,
    }


def validate_metrics(results):
    for row in results:
        for metric in ["AUC", "Accuracy", "F1", "Precision", "Recall"]:
            value = row[metric]
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{row['Model']} invalid {metric}: {value}")


def print_results(results):
    print("Task 6: Model comparison")
    print(f"{'Model':<22} {'AUC':>7} {'Acc':>7} {'F1':>7} {'Prec':>7} {'Recall':>7} {'Time':>8} {'TN':>4} {'FP':>4} {'FN':>4} {'TP':>4}")
    print("-" * 92)
    for row in results:
        print(
            f"{row['Model']:<22} {row['AUC']:>7.3f} {row['Accuracy']:>7.3f} "
            f"{row['F1']:>7.3f} {row['Precision']:>7.3f} {row['Recall']:>7.3f} "
            f"{row['Training Time']:>8.2f} {row['TN']:>4} {row['FP']:>4} "
            f"{row['FN']:>4} {row['TP']:>4}"
        )


def print_importances(rf_model):
    importances = list(rf_model.stages[-1].featureImportances)
    pairs = sorted(zip(FEATURE_NAMES, importances), key=lambda item: item[1], reverse=True)
    print("Task 7: Random Forest feature importance")
    for name, value in pairs:
        print(f"{name:<16} {value:.4f}")
    print(f"Most important feature: {pairs[0][0]}")


def main():
    data_path = sys.argv[1] if len(sys.argv) > 1 else "hdfs:///data/chicago_crimes_sample.csv"

    spark = (
        SparkSession.builder.appName("M2_Task11_Spark_ML")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("Tasks 5-7 and Task 11: Spark ML Pipeline")
    print("Task 5 Author: Ibrahim Alhagbani")
    print("Task 6 Author: Abdulaziz Albaz")
    print("Task 7 Author: Nawaf Alshuaibi")
    print("Task 11 Author: Mishari Al Mogren")
    print(f"Master: {spark.sparkContext.master}")
    print(f"Data path: {data_path}")

    df = prepare_data(spark, data_path).cache()
    print(f"Rows after cleaning: {df.count()}")
    df.groupBy("label").count().orderBy("label").show()

    stages = feature_stages()
    feature_model = Pipeline(stages=stages).fit(df)
    feature_df = feature_model.transform(df)

    print("Task 5: Feature vector sample")
    print("Author: Ibrahim Alhagbani")
    feature_df.select(
        "Primary Type",
        "District",
        "Hour",
        "Domestic_str",
        "crime_index",
        "domestic_index",
        "features",
        "label",
    ).show(5, truncate=False)
    print("Feature vector order: [District, crime_index, Hour, domestic_index]")

    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    train_df.cache()
    test_df.cache()
    print(f"Training rows: {train_df.count()}")
    print(f"Testing rows: {test_df.count()}")

    models = [
        (
            "Logistic Regression",
            LogisticRegression(featuresCol="features", labelCol="label", maxIter=100, regParam=0.01),
        ),
        (
            "Random Forest",
            RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=100, maxDepth=5, seed=42),
        ),
        (
            "GBT",
            GBTClassifier(featuresCol="features", labelCol="label", maxIter=50, maxDepth=5, seed=42),
        ),
    ]

    trained = {}
    results = []
    for name, classifier in models:
        print(f"Training {name}")
        model, row = train_and_score(name, classifier, stages, train_df, test_df)
        trained[name] = model
        results.append(row)

    validate_metrics(results)
    print("Author: Abdulaziz Albaz")
    print_results(results)
    print("Author: Nawaf Alshuaibi")
    print_importances(trained["Random Forest"])
    print("Author: Mishari Al Mogren")
    print("Task 11 complete")

    train_df.unpersist()
    test_df.unpersist()
    df.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()

