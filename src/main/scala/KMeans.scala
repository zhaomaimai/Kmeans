import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{SparkSession, DataFrame, Row}

object KMeans {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf并设置应用程序名称
    val sparkConf = new SparkConf().setAppName("KMeans")

    // 创建SparkSession
    val spark = SparkSession.builder.config(sparkConf).getOrCreate()

    // 读取数据
    val dataRDD = spark.sparkContext.textFile("/home/hadoop/data/kmeans_data.txt")

    // 将数据转换为DataFrame
    val data: DataFrame = spark.createDataFrame(
      dataRDD.map(line => line.split(" ").map(_.toDouble))
        .map(values => Row(values(0), values(1), values(2))),
      StructType(Seq(
        StructField("feature1", DoubleType, nullable = true),
        StructField("feature2", DoubleType, nullable = true),
        StructField("feature3", DoubleType, nullable = true)
      ))
    )

    // 特征向量化，将特征列组合成一个向量列
    val featureCols = Array("feature1", "feature2", "feature3")
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val assembledData = assembler.transform(data)

    // 创建KMeans模型
    val means = new KMeans().setK(3) // 设置簇的个数，这里假设为3
    val model: KMeansModel = means.fit(assembledData)

    // 获取聚类结果
    val predictions = model.transform(assembledData)

    // 打印聚类结果
    predictions.select("features", "prediction").show()

    // 打印簇中心
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)

    // 关闭SparkSession
    spark.stop()
  }
}
