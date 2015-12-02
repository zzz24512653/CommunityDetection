import org.apache.spark.graphx.Graph

object CommUtil {

  // 返回网络中的社区集合
  def getCommunities(G:Graph[VertexData,Double]): Unit = {
    println("=========== current communities ===========")
    //G.vertices.foreach(x=>println(x._1+"     "+x._2.innerVertices.mkString(" ")))

    val communities = G.vertices.map(x=>{
      val innerVertices = x._2.innerVertices
      val cid = x._2.cId
      (cid,innerVertices)
    }).groupByKey.map(x=>{
      val cid = x._1
      val vertices = x._2.flatten.toSet
      //println("cid: " + cid + " " + x._2.mkString(" "))
      (cid,vertices)
    })
    communities.foreach(println)
    println()

    communities
  }

}