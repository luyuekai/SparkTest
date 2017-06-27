/**
  * Created by lyk on 6/22/17.
  */
object Test0621 {

  def isHeader(line: String): Boolean = {
    line.contains("id_1")
  };

  def toDouble(s: String): Double = {
    if ("?".equals(s)) {
      Double.NaN
    }
    else {
      s.toDouble
    }
  }

  def parse(line: String) = {
    val pieces = line.split(",")
    val id1 = pieces(0).toInt
    val id2 = pieces(1).toInt
    val scores = pieces.slice(2, 11).map(toDouble)
    val matched = pieces(11).toBoolean
//    (id1,id2,scores,matched)
    MatchData(id1,id2,scores,matched)
  }

  case class MatchData(id1:Int,id2:Int,scores:Array[Double],matched:Boolean)

}
