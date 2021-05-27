package spark

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory

/**
 * Various utility classes for working with Akka.
 */
private[spark] object AkkaUtils {
  /**
   * Creates an ActorSystem ready for remoting, with various Spark features. Returns both the
   * ActorSystem itself and its port (which is hard to get from Akka).
   */
  def createActorSystem(name: String, host: String, port: Int): ActorSystem = {
    val akkaThreads = System.getProperty("spark.akka.threads", "4").toInt
    val akkaBatchSize = System.getProperty("spark.akka.batchSize", "15").toInt
    val akkaTimeout = System.getProperty("spark.akka.timeout", "20").toInt
    val akkaFrameSize = System.getProperty("spark.akka.frameSize", "10").toInt
    val akkaConf = ConfigFactory.parseString(
      """
      akka.daemonic = on
      akka.event-handlers = ["akka.event.slf4j.Slf4jEventHandler"]
      akka.actor.provider = "akka.remote.RemoteActorRefProvider"
      akka.remote.transport = "akka.remote.netty.NettyRemoteTransport"
      akka.remote.log-remote-lifecycle-events = on
      akka.remote.netty.hostname = "%s"
      akka.remote.netty.port = %d
      akka.remote.netty.connection-timeout = %ds
      akka.remote.netty.message-frame-size = %d MiB
      akka.remote.netty.execution-pool-size = %d
      akka.actor.default-dispatcher.throughput = %d
      """.format(host, port, akkaTimeout, akkaFrameSize, akkaThreads, akkaBatchSize))

    val actorSystem = ActorSystem("spark", akkaConf, getClass.getClassLoader)

    // Figure out the port number we bound to, in case port was passed as 0. This is a bit of a
    // hack because Akka doesn't let you figure out the port through the public API yet.
    actorSystem
  }

}