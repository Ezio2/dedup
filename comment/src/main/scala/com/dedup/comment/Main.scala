package com.dedup.comment

import java.util.concurrent.TimeUnit
import java.{lang, util}

import com.dedup.comment.data.Comment
import com.dedup.thrift.comment.{DedupComment, Req}
import com.dedup.util.{TimeMeasure, IdGenerator}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.thrift.server.TThreadPoolServer
import org.apache.thrift.transport.TServerSocket

import scala.collection.JavaConversions._
import scala.collection.mutable

class ServerHandler(handler: Handler) extends DedupComment.Iface {
  private val log = Logger.getLogger(this.getClass.getSimpleName)
  override def dedup(reqs: util.List[Req]): util.Map[lang.Long, lang.Long] = {
    implicit val profile = mutable.Map[String, Long]()

    val comments = TimeMeasure.profile("parseComments") {
      reqs.par.flatMap(r => Comment(r.id, r.content, r.getCreateTime)).toList
    }
    val r = handler.synchronized {
      handler.handler(comments).map(x => Long.box(x._1) -> Long.box(x._2))
    }
    TimeMeasure.logProfile(profile, "dedup")
    log.info(s"get ${reqs.size} reqs, throughput: ${reqs.size / profile("total").toDouble * 1000}")
    r
  }
}

object Main extends App {
  private val conf = ConfigFactory.load("comment")
  System.setProperty("LOG_DIR", conf.getString("log_home"))
  PropertyConfigurator.configure(getClass.getResource("/comment.properties"))
  private val log = Logger.getLogger(this.getClass.getSimpleName)

  val handler = new Handler(
    conf.getString("rocksdb.forwardIndex.pathPrefix"),
    conf.getInt("rocksdb.forwardIndex.cacheSize"),
    conf.getDuration("rocksdb.forwardIndex.ttl", TimeUnit.MILLISECONDS),
    conf.getString("rocksdb.invertIndex.pathPrefix"),
    conf.getInt("rocksdb.invertIndex.cacheSize"),
    conf.getDuration("rocksdb.invertIndex.ttl", TimeUnit.MILLISECONDS),
    new IdGenerator(conf.getString("idGeneratorUrl")),
    conf.getDouble("threshold.jaccard.t"),
    conf.getDouble("threshold.levenshtein.filter_jaccard"),
    conf.getDouble("threshold.levenshtein.t")
  )


  val processor = new DedupComment.Processor(new ServerHandler(handler))
  val serverTransport = new TServerSocket(conf.getInt("port"))
  val a = new TThreadPoolServer.Args(serverTransport).processor(processor)
  a.maxWorkerThreads(conf.getInt("threadNum"))
  val server = new TThreadPoolServer(a)

  log.info(s"comment dedup start at port:${conf.getInt("port")} threads:${conf.getInt("threadNum")}")
  server.serve()

}
