package spark

import akka.actor.ActorSystem

class SparkEnv(
                val actorSystem: ActorSystem,
                val serializer: Serializer,
                val closureSerializer: Serializer,
                val cacheTracker: CacheTracker,
                val mapOutputTracker: MapOutputTracker,
                val shuffleFetcher: ShuffleFetcher,
                val shuffleManager: ShuffleManager,
                val cache: Cache,
              )

object SparkEnv {
  private val env = new ThreadLocal[SparkEnv]

  def set(e: SparkEnv) {
    env.set(e)
  }

  def get: SparkEnv = {
    env.get()
  }

  def createFromSystemProperties(isMaster: Boolean): SparkEnv = {

    val hostname = System.getProperty("spark.master.host")
    val port = Integer.parseInt(System.getProperty("spark.master.port"))

    val cacheClass = System.getProperty("spark.cache.class", "spark.BoundedMemoryCache")
    val cache = Class.forName(cacheClass).newInstance().asInstanceOf[Cache]

    val serializerClass = System.getProperty("spark.serializer", "spark.JavaSerializer")
    val serializer = Class.forName(serializerClass, true, Thread.currentThread.getContextClassLoader).newInstance().asInstanceOf[Serializer]

    val closureSerializerClass =
      System.getProperty("spark.closure.serializer", "spark.JavaSerializer")
    val closureSerializer =
      Class.forName(closureSerializerClass).newInstance().asInstanceOf[Serializer]

    val actorSystem = AkkaUtils.createActorSystem("spark", hostname, port)

    val cacheTracker = new CacheTracker(actorSystem, isMaster, cache)

    val mapOutputTracker = new MapOutputTracker(actorSystem, isMaster)

    val shuffleFetcherClass =
      System.getProperty("spark.shuffle.fetcher", "spark.SimpleShuffleFetcher")
    val shuffleFetcher =
      Class.forName(shuffleFetcherClass).newInstance().asInstanceOf[ShuffleFetcher]

    val shuffleMgr = new ShuffleManager()

    new SparkEnv(
      actorSystem,
      serializer,
      closureSerializer,
      cacheTracker,
      mapOutputTracker,
      shuffleFetcher,
      shuffleMgr,
      cache)
  }
}
