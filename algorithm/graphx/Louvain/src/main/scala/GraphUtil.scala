import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.{ArrayBuffer, HashSet}

object GraphUtil {

  // 加载原始图
  def loadInitGraph(sc:SparkContext, path:String):Graph[None.type,Double] = {
    val data = sc.textFile(path)
    val edges = data.map(line => {
      val items = line.split(" ")
      Edge(items(0).toLong, items(1).toLong, 1.0)
    })
    Graph.fromEdges(edges,None)
  }

  // 构建Louvain算法所需使用的图结构
  def createLouvainGraph(initG:Graph[None.type,Double]):Graph[VertexData,Double] = {
    // sum of the weights of the links incident to node i
    val nodeWeights:VertexRDD[Double] = initG.aggregateMessages(
      trip => {
        trip.sendToSrc(trip.attr)
        trip.sendToDst(trip.attr)
      },
      (a,b) => a + b
    )
    val louvainG = initG.outerJoinVertices(nodeWeights)((vid, oldData, opt) => {
      val vData = new VertexData(vid,vid)
      val weights = opt.getOrElse(0.0)
      vData.degree = weights
      vData.innerVertices += vid
      vData.commVertices += vid
      vData
    })

    //louvainG.vertices.foreach(x=>println(x._1+" "+x._2.kIn+" "+x._2.communityNodes.mkString(" ")))
    louvainG
  }

  // 更新网络中结点的社区编号以及这个社区中所包含的结点
  def updateGraph(G:Graph[VertexData,Double], changeInfo:RDD[(VertexId,Long)]):Graph[VertexData,Double] = {
    // 更新社区编号
    var newG = G.joinVertices(changeInfo)((vid,data,newCid) => {
      val vData = new VertexData(vid,newCid)
      vData.innerDegree = data.innerDegree
      vData.innerVertices = data.innerVertices
      vData.degree = data.degree
      vData
    })

    val updateInfo = newG.vertices.map(x=>{
      val vid = x._1
      val cid = x._2.cId
      (cid,vid)
    }).groupByKey.flatMap(x=>{
      val vertices = x._2
      vertices.map(vid=>(vid,vertices))
    })

    newG = newG.joinVertices(updateInfo)((vid,data,opt)=>{
      val vData = new VertexData(vid,data.cId)
      val cVertices = new HashSet[VertexId]()
      for (vid <- opt)
        cVertices += vid
      vData.innerDegree = data.innerDegree
      vData.innerVertices = data.innerVertices
      vData.degree = data.degree
      vData.commVertices = cVertices
      vData
    })

    newG
  }

}