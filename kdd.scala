
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD

/**
  * Created by lyk on 6/27/17.
  */
object kdd {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("kdd").setMaster("local");
    val sc = new SparkContext(conf)
    val rawRDD = sc.textFile("hdfs://192.168.80.32/user/luyuekai/kdd/")

    rawRDD.map(_.split(",").last).countByValue().toSeq.sortBy(_._2).reverse.foreach(println)

    val labellsAndData = rawRDD.map {
      line =>
        val buffer = line.split(",").toBuffer
        buffer.remove(1, 3)
        val label = buffer.remove(buffer.length - 1)
        val vector = Vectors.dense(buffer.map(_.toDouble).toArray)
        (label, vector)
    }
    val data = labellsAndData.values.cache();

//    val kmeans = new KMeans();
//    val model = kmeans.run(data);

//    model.clusterCenters.foreach(println)
//
//    val clusterLabelCount = labellsAndData.map{
//      case(label,datum) =>
//        val cluster = model.predict(datum)
//        (cluster,label)
//    }.countByValue()
//
//    clusterLabelCount.toSeq.sorted.foreach{
//      case ((cluster,label),count) =>
//        println(f"$cluster%1s$label%18s$count%8s")
//    }

  }

  def distance(a:Vector,b:Vector) = math.sqrt(a.toArray.zip(b.toArray).map(p=>p._1-p._2).map(d=>d*d).sum)

  def distToCenteroid(datum:Vector,model:KMeansModel) = {
    val cluster = model.predict(datum)
    val centroid = model.clusterCenters(cluster)
    distance(centroid,datum)
  }

  def clusteringScore(data: RDD[Vector],k:Int)={
    val kmeans = new KMeans()
    kmeans.setK(k)
    val model = kmeans.run(data)
    model.clusterCenters.foreach(println)
    data.map(datum=>distToCenteroid(datum,model)).mean()
  }


}
