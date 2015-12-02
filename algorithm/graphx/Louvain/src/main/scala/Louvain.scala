import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.{HashSet, HashMap, ArrayBuffer}

class VertexData(val vId:Long, var cId:Long) extends Serializable {
    var innerDegree = 0.0                       //内部结点的权重
    var innerVertices = new HashSet[Long]()     //内部的结点
    var degree = 0.0                            //结点的度
    var commVertices = new HashSet[Long]()      //社区中的结点
}

object Louvain {

    // 汇总结点的邻居社区信息用于计算modularity
    def getNeighCommInfo(G:Graph[VertexData,Double]): RDD[(VertexId,Iterable[(Long,Double,Double)])] = {

        //　求出每个社区的tot
        val commTot = G.vertices.map(v=>(v._2.cId,v._2.degree+v._2.innerDegree)).groupByKey().map(x=>{
            val cid = x._1
            val tot = x._2.sum
            (cid,tot)
        })

        // 求出每个结点对于它的邻居社区的k_in
        val commKIn = G.triplets.flatMap(trip => {
            val weight = trip.attr
            Array((trip.srcAttr.cId,(trip.dstId->weight)),(trip.dstAttr.cId,(trip.srcId,trip.attr)))
        }).groupByKey().map(t=>{
            val cid = t._1
            val m = new HashMap[VertexId,Double]()
            for (x <- t._2) {
                if (m.contains(x._1))
                    m(x._1) += x._2
                else
                    m(x._1) = x._2
            }
            (cid,m)
        })

        // 结点的邻居社区的tot以及这个结点的k_i_in
        val neighCommInfo = commTot.join(commKIn).flatMap(x => {
            val cid = x._1
            val tot = x._2._1
            x._2._2.map(t => {
                val vid = t._1
                val k_in = t._2
                (vid,(cid,k_in,tot))
            })
        }).groupByKey()

        neighCommInfo  //(vid,(cid,k_i_in,tot))
    }

    // 计算每个结点的最大modularity增加
    def getChangeInfo(G:Graph[VertexData,Double], neighCommInfo:RDD[(VertexId,Iterable[(Long,Double,Double)])], m:Double):RDD[(VertexId,Long,Double)] = {
        val changeInfo = G.vertices.join(neighCommInfo).map(x =>{
            val vid = x._1
            val data = x._2._1
            val commIter = x._2._2           //邻居社区
            val vCid = data.cId              //结点当前的社区ID
            val k_v = data.degree + data.innerDegree
            val maxQ = commIter.map(t=>{
                val nCid = t._1              //邻居社区ID
                val k_v_in = t._2
                var tot = t._3
                if (vCid == nCid)            //如果已经在社区中，需减去结点的度信息
                    tot -= k_v
                val q = (k_v_in - tot * k_v / m)
                (vid,nCid,q)
            }).max(Ordering.by[(VertexId,Long,Double),Double](_._3))

            if (maxQ._3 > 0.0) maxQ else (vid,vCid,0.0)
         })

        changeInfo //(vid,vCid,wCid,q)
    }

    // Louvain算法的第一步
    def step1(louvainG:Graph[VertexData,Double], m:Double):(Graph[VertexData,Double],Int) = {
        println("==================== step 1 ====================")
        var G = louvainG
        var iterTime = 0
        var canStop = false
        while (iterTime < 100 && !canStop) {
            val neighborComm = getNeighCommInfo(G)
            val changeInfo = getChangeInfo(G,neighborComm, m)
            val changeCount = G.vertices.zip(changeInfo).filter(x=>
                x._1._2.cId != x._2._2
            ).count()
            // 用连通图来解决社区归属延迟问题
            val newChangeInfo = Graph.fromEdgeTuples(changeInfo.map(x=>(x._1,x._2)), 0).connectedComponents().vertices
            G = GraphUtil.updateGraph(G,newChangeInfo)
            if (changeCount==0)
                canStop = true
            else {
                val newChangeInfo = Graph.fromEdgeTuples(changeInfo.map(x=>(x._1,x._2)), 0).connectedComponents().vertices
                G = GraphUtil.updateGraph(G,newChangeInfo)
                iterTime += 1
            }
        }
        (G,iterTime)
    }

    // Louvain算法的第二步
    def step2(G:Graph[VertexData,Double]): Graph[VertexData,Double] = {
        println("============================== step 2 =======================")
        val edges = G.triplets.filter(trip => trip.srcAttr.cId != trip.dstAttr.cId).map(trip => {
            val cid1 = trip.srcAttr.cId
            val cid2 = trip.dstAttr.cId
            val weight = trip.attr
            ((math.min(cid1,cid2),math.max(cid1,cid2)),weight)
        }).groupByKey().map(x=>Edge(x._1._1,x._1._2,x._2.sum))

        val initG = Graph.fromEdges(edges, None)
        var louvainGraph = GraphUtil.createLouvainGraph(initG)

        val oldKIn = G.vertices.map(v=>(v._2.cId,(v._2.innerVertices,v._2.innerDegree))).groupByKey().map(x=>{
            val cid = x._1
            val vertices = x._2.flatMap(t=>t._1).toSet[VertexId]
            val kIn = x._2.map(t => t._2).sum
            (cid,(vertices,kIn))
        })

        val newKIn = G.triplets.filter(trip => trip.srcAttr.cId == trip.dstAttr.cId).map(trip => {
            val cid = trip.srcAttr.cId
            val vertices1 = trip.srcAttr.innerVertices
            val vertices2 = trip.dstAttr.innerVertices
            val weight = trip.attr * 2
            (cid,(vertices1.union(vertices2),weight))
        }).groupByKey().map(x => {
            val cid = x._1
            val vertices = x._2.flatMap(t => t._1).toSet[VertexId]
            val kIn = x._2.map(t => t._2).sum
            (cid,(vertices,kIn))
        })

        // 超结点信息
        val superVertexInfo = oldKIn.union(newKIn).groupByKey().map(x => {
            val cid = x._1
            val vertices = x._2.flatMap(t => t._1).toSet[VertexId]
            val kIn = x._2.map(t => t._2).sum
            (cid,(vertices,kIn))
        })

        louvainGraph = louvainGraph.outerJoinVertices(superVertexInfo)((vid,data,opt) => {
            var innerVerteices = new HashSet[VertexId]()
            val kIn = opt.get._2
            for (vid <- opt.get._1)
                innerVerteices += vid
            data.innerVertices = innerVerteices
            data.innerDegree = kIn
            data
        })

        louvainGraph
    }

    def execute(sc:SparkContext, initG:Graph[None.type,Double]) {

        var louvainG = GraphUtil.createLouvainGraph(initG)
        // sum of weights of all links in the network
        val m = sc.broadcast(louvainG.edges.map(e=>e.attr).sum())

        var curIter = 0
        var res = step1(louvainG, m.value)
        while (res._2 != 0 && curIter < 20) {
            louvainG = res._1
            louvainG = step2(louvainG)
            CommUtil.getCommunities(louvainG)
            res = step1(louvainG, m.value)
            curIter += 1
        }

    }

    def main(args: Array[String]) {
        val conf = new SparkConf()
        conf.setAppName("Louvain")
        conf.setMaster("local[2]")
        val sc = new SparkContext(conf)
        val initG = GraphUtil.loadInitGraph(sc, "/home/zzz/desktop/club.txt")
        execute(sc, initG)
    }

}