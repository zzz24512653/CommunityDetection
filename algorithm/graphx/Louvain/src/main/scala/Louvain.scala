import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.{HashSet, HashMap, ArrayBuffer}

class VertexData(val vId:Long, var cId:Long) extends Serializable {
    var kIn = 0.0                                 //内部结点的权重
    var innerVertices = new HashSet[Long]()       //内部的结点
    var degree = 0.0                              //结点的度
    var neighbors = new HashSet[VertexId]()       //结点的邻居
    var neighborWeights = 0.0                     //邻居的总权重
    var communityVertices = new HashSet[Long]()   //社区中的结点
}

object Louvain {

    def loadInitGraph(sc:SparkContext, path:String):Graph[None.type,Double] = {
        val data = sc.textFile(path)
        val edges = data.map(line => {
            val items = line.split(" ")
            Edge(items(0).toLong, items(1).toLong, 1.0)
        })
        Graph.fromEdges(edges,None)
    }

    def createLouvainGraph(initG:Graph[None.type,Double]):Graph[VertexData,Double] = {
        // sum of the weights of the links incident to node i
        val nodeNeighborsWeights:VertexRDD[(HashSet[VertexId],Double)] = initG.aggregateMessages(
            trip => {
                trip.sendToSrc((HashSet(trip.dstId),trip.attr))
                trip.sendToDst((HashSet(trip.srcId),trip.attr))
            },
            (a,b) => (a._1 ++ b._1, a._2+b._2)
        )
        val louvainG = initG.outerJoinVertices(nodeNeighborsWeights)((vid, data, opt) => {
            val vertexData = new VertexData(vid,vid)
            val neighborsAndWeights = opt.getOrElse((HashSet[VertexId](),0.0))
            vertexData.neighbors = neighborsAndWeights._1
            vertexData.degree = neighborsAndWeights._2
            vertexData.neighborWeights = neighborsAndWeights._2
            vertexData.innerVertices += vid
            vertexData.communityVertices += vid
            vertexData
        })

        //louvainG.vertices.foreach(x=>println(x._1+" "+x._2.kIn+" "+x._2.communityNodes.mkString(" ")))
        louvainG
    }

    // aggregate neighbor community info
    def getNeighCommInfo(G:Graph[VertexData,Double]): RDD[(VertexId,Iterable[(Long,Double,Double)])] = {

        //　求出每个社区的tot
        val commTot = G.vertices.map(v=>(v._2.cId,v._2.degree+v._2.kIn)).groupByKey().map(x=>{
            val cid = x._1
            val tot = x._2.sum
            (cid,tot)
        })
        println("================= community tot info ==============")
        commTot.foreach(println)

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

        val neighCommInfo = commTot.join(commKIn).flatMap(x => {
            val cid = x._1
            val tot = x._2._1
            x._2._2.map(t => {
                val vid = t._1
                val k_in = t._2
                (vid,(cid,k_in,tot))
            })
        }).groupByKey()

        println("=================== neighbor info ===================")
        neighCommInfo.foreach(x=>println(x._1+" "+x._2.mkString(" ")))
        //commKIn.foreach(x=>println(x._1+" " + x._2.mkString(" ")))

        neighCommInfo  //(vid,(cid,k_i_in,tot))
    }

    // cal max modularity gain of each node
    def getChangeInfo(G:Graph[VertexData,Double], neighCommInfo:RDD[(VertexId,Iterable[(Long,Double,Double)])], m:Double):RDD[(VertexId,Long,Double)] = {
        val changeInfo = G.vertices.join(neighCommInfo).map(x =>{
            val vid = x._1
            val data = x._2._1
            val commIter = x._2._2
            val vCid = data.cId              //结点当前的社区ID
            val k_v = data.degree + data.kIn

            val maxQ = commIter.map(t=>{
                val nCid = t._1              //邻居社区ID
                val k_v_in = t._2
                var tot = t._3
                //println("k_v: " + k_v + " tot: " + tot)
                if (vCid == nCid)
                    tot -= k_v
                val q = (k_v_in - tot * k_v / m)
                //println("vid: " + vid + " vcid: " + vCid + " ncid: " + nCid  + " tot: " + tot + " q: " + q)
                (vid,nCid,q)
            }).max(Ordering.by[(VertexId,Long,Double),Double](_._3))

            //println("maxQ: " + maxQ)
            //println()
            if (maxQ._3 > 0.0) maxQ else (vid,vCid,0.0)
            //println(res)
            //println(vid + " neighbors: " + neighbors + " commInfo: " + neighCommunities.mkString(" "))
        })

        println("======================== change info ========================")
        val changeVertices = G.vertices.zip(changeInfo).filter(x=>
            x._1._2.cId != x._2._2
        )
        G.vertices.zip(changeInfo).filter(x=>
            x._1._2.cId == x._2._2
        ).foreach(x=>println("xxxxx vid: "+x._1._1+" old cid: "+x._1._2.cId+" new cid: "+x._2._2))
        changeVertices.foreach(x=>println("vid: "+x._1._1+" old cid: "+x._1._2.cId+" new cid: "+x._2._2))
        println("======================== change count: "+changeVertices.count()+"========================")
        println()

        changeInfo //(vid,vCid,wCid,q)
    }

    // update tot,communities
    def updateGraph(G:Graph[VertexData,Double], changeInfo:RDD[(VertexId,Long)]):Graph[VertexData,Double] = {
        var newG = G.joinVertices(changeInfo)((vid,data,newCid) => {
            val newData = new VertexData(vid,newCid)
            newData.kIn = data.kIn
            newData.innerVertices = data.innerVertices
            newData.degree = data.degree
            newData.neighbors = data.neighbors
            newData
        })

        val updateInfo = newG.vertices.map(x=>{
            val vid = x._1
            val cid = x._2.cId
            val degree = x._2.degree
            (cid,(vid,degree))
        }).groupByKey.map(x=>{
            val infoIter = x._2
            val vertices = new HashSet[VertexId]()
            var tot = 0.0
            for (info <- infoIter) {
                vertices += info._1
                tot += info._2
            }
            (vertices,tot)
        }).flatMap(x=>{
            val vertices = x._1
            val tot = x._2
            val buff = new ArrayBuffer[(VertexId,(HashSet[VertexId],Double))]()
            for (vid <- vertices) {
                buff ++= ArrayBuffer((vid,(vertices,tot)))
            }
            buff
        })

        //println("============== update info =============")
        //updateInfo.foreach(println)

        newG = newG.joinVertices(updateInfo)((vid,data,opt)=>{
            val newData = new VertexData(vid,data.cId)
            val cVertices = opt._1
            val tot = opt._2
            newData.kIn = data.kIn
            newData.innerVertices = data.innerVertices
            newData.degree = data.degree
            newData.neighbors = data.neighbors
            newData.communityVertices = cVertices
            newData.neighborWeights = tot
            newData
        })

        //newG.vertices.foreach(x=>println("vid: " + x._1 + " cid: " + x._2.cId + " cvertices: " + x._2.communityVertices + " ctot: " + x._2.tot))
        //println()

        //println("============== new Graph info =============")
        //newG.triplets.foreach(trip=>println("srcid: " + trip.srcId+" dstid: "+trip.dstId+" srccid: "+trip.srcAttr.cId+" dstcid: "+trip.dstAttr.cId + " src tot: " + trip.srcAttr.tot))
        //println()

        newG
    }

    def getCommunities(G:Graph[VertexData,Double]): Unit = {
        println("=============================== current communities =======================")
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
    }

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
            val newChangeInfo = Graph.fromEdgeTuples(changeInfo.map(x=>(x._1,x._2)), 0).connectedComponents().vertices
            G = updateGraph(G,newChangeInfo)
            if (changeCount==0)
                canStop = true
            else {
                val newChangeInfo = Graph.fromEdgeTuples(changeInfo.map(x=>(x._1,x._2)), 0).connectedComponents().vertices
                G = updateGraph(G,newChangeInfo)
                iterTime += 1
            }
            println(" =================== communities in step 1 ======================")
            getCommunities(G)
        }
        (G,iterTime)
    }

    def step2(G:Graph[VertexData,Double]): Graph[VertexData,Double] = {
        println("============================== step 2 ==============================")
        val edges = G.triplets.filter(trip => trip.srcAttr.cId != trip.dstAttr.cId).map(trip => {
            val cid1 = trip.srcAttr.cId
            val cid2 = trip.dstAttr.cId
            val weight = trip.attr
            ((math.min(cid1,cid2),math.max(cid1,cid2)),weight)
        }).groupByKey().map(x=>Edge(x._1._1,x._1._2,x._2.sum))
        //edges.foreach(x=>println(x))
        val initG = Graph.fromEdges(edges, None)
        println("===================== louvain graph =================")

        var louvainGraph = createLouvainGraph(initG)
        //println("===================== init louvain graph =====================")
        //louvainGraph.vertices.foreach(x=>println("vid: "+x._1+" inner vertices: "+x._2.communityVertices+" kin: " + x._2.kIn+" degree: "+x._2.degree))


        println("=============== 计算社区内结点本身的kIn ==============")
        val oldKIn = G.vertices.map(v=>(v._2.cId,(v._2.innerVertices,v._2.kIn))).groupByKey().map(x=>{
            val cid = x._1
            val vertices = x._2.flatMap(t=>t._1).toSet[VertexId]
            val kIn = x._2.map(t => t._2).sum
            (cid,(vertices,kIn))
        })

        println("=============== 计算kIn的增加 =============")
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

        val superVertexInfo = oldKIn.union(newKIn).groupByKey().map(x => {
            val cid = x._1
            val vertices = x._2.flatMap(t => t._1).toSet[VertexId]
            val kIn = x._2.map(t => t._2).sum
            (cid,(vertices,kIn))
        })

        println("===================== super vertex info ====================")
        println("================== super vertex num: " + superVertexInfo.count() + " =================")
        superVertexInfo.foreach(println)

        louvainGraph = louvainGraph.outerJoinVertices(superVertexInfo)((vid,data,opt) => {
            var innerVerteices = new HashSet[VertexId]()
            val kIn = opt.get._2
            for (vid <- opt.get._1)
                innerVerteices += vid
            data.innerVertices = innerVerteices
            data.kIn = kIn
            data
        })
        println("========================= super louvain graph ======================")
        louvainGraph.vertices.foreach(x=>println("vid: "+x._1+" inner vertices: "+x._2.innerVertices+" kin: " + x._2.kIn+" degree: "+x._2.degree))

        louvainGraph
    }

    def execute(sc:SparkContext, initG:Graph[None.type,Double]) {

        var louvainG = createLouvainGraph(initG)
        // sum of weights of all links in the network
        val m = sc.broadcast(louvainG.edges.map(e=>e.attr).sum())
        //println("============= m : " + m + " =============")

        var curIter = 0
        var res = step1(louvainG, m.value)
        while (res._2 != 0 && curIter < 20) {
            louvainG = res._1
            louvainG = step2(louvainG)
            getCommunities(louvainG)
            res = step1(louvainG, m.value)
            curIter += 1
        }

    }

    def main(args: Array[String]) {
        val conf = new SparkConf()
        conf.setAppName("Louvain")
        conf.setMaster("local[2]")
        val sc = new SparkContext(conf)
        val initG = loadInitGraph(sc, "/home/zzz/desktop/club.txt")
        execute(sc, initG)
    }

}