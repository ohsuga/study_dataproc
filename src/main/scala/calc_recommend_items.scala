import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration
import com.google.cloud.hadoop.io.bigquery.BigQueryOutputFormat
import com.google.cloud.hadoop.io.bigquery.GsonBigQueryInputFormat
import com.google.gson.JsonObject
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable

object CalcRecommendItems{

  // 入力パラメータ
  val fullyQualifiedInputTableId = "work.user_item_matrix"

  // 出力パラメータ
  val projectId = "ml20161121"
  val outputDatasetId = "dataset"
  val outputTableId = "outputdata"
  val outputTableSchema =
    "[{'name':'user_id','type':'STRING'},{'name':'item_id','type': 'STRING'},{'name': 'rating','type': 'FLOAT'}]"

  // JSONをListに変換するヘルパー
  def convertToList(record: JsonObject) : Array[String] = {
    val user_id = record.get("user_id").getAsString
    val item_id = record.get("item_id").getAsString
    val score = record.get("score").getAsString
    return Array(user_id, item_id, score)
  }

  // RatingオブジェクトをJSONに変換するヘルパー
  def convertToJson(rate: Rating) : JsonObject = {
    val user_id = rate.user.toString
    val item_id = rate.product.toString
    val rating = rate.rating.toDouble

    val jsonObject = new JsonObject()
    jsonObject.addProperty("user_id", user_id)
    jsonObject.addProperty("item_id", item_id)
    jsonObject.addProperty("rating", rating)

    return jsonObject
  }

  def calc_recommend_items(rank: Int=300, numBlocks: Int=15, iterations: Int=10, numItems: Int=50, lambda: Double = 0.01, alpha: Double = 0.01) {

    // SparkContext インスタンスを作成して必要情報を取得
    val sc = new SparkContext()
    val conf = sc.hadoopConfiguration
    val bucket = conf.get("fs.gs.system.bucket")

    // インプットの設定
    conf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId)
    conf.set(BigQueryConfiguration.GCS_BUCKET_KEY, bucket)
    BigQueryConfiguration.configureBigQueryInput(conf, fullyQualifiedInputTableId)

    val inputTmpDir = s"gs://${bucket}/hadoop/tmp/bigquery/tmp_user_item_matrix_export"
    conf.set(BigQueryConfiguration.TEMP_GCS_PATH_KEY, inputTmpDir)

    // アウトプットの設定
    BigQueryConfiguration.configureBigQueryOutput(
      conf, projectId, outputDatasetId, outputTableId, outputTableSchema)
    conf.set(
      "mapreduce.job.outputformat.class",
      classOf[BigQueryOutputFormat[_,_]].getName)

    // BigQueryからデータを読み込む
    val tableData = sc.newAPIHadoopRDD(
      conf,
      classOf[GsonBigQueryInputFormat],
      classOf[LongWritable],
      classOf[JsonObject]).cache

    // BigQueryから読み込んだデータをRatingに変換
    val ratings = tableData.map(x => convertToList(x._2)).map(_ match { case Array(user, product, rating) =>
      Rating(user.toInt, product.toInt, rating.toDouble)
    })

    // ALS で レコメンドモデルを構築
    val model = ALS.trainImplicit(ratings, rank, iterations, alpha, numBlocks, lambda)
    // ユーザーごとの推薦アイテムを計算
    val recommendItemsList = model.recommendProductsForUsers(numItems)
    // BigQuery へ書き込みできる形式に変換
    val outRDD = recommendItemsList.map(x => x._2).flatMap(x => x).map(x => (null,convertToJson(x)))
    // BigQuery へ書き込み
    outRDD.saveAsNewAPIHadoopDataset(conf)

    val inputTmpDirPath = new Path(inputTmpDir)
    inputTmpDirPath.getFileSystem(conf).delete(inputTmpDirPath, true)
  }

  def main(args: Array[String]){
    calc_recommend_items()
  }

}

