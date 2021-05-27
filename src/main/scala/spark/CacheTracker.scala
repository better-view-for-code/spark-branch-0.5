package spark

import akka.actor._
import akka.dispatch._
import akka.pattern.ask
import akka.remote._
import akka.util.Timeout

import scala.concurrent.duration._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.concurrent.Await
import akka.util.Timeout

private[spark] sealed trait CacheTrackerMessage

private[spark] case class AddedToCache(rddId: Int, partition: Int, host: String, size: Long = 0L)
  extends CacheTrackerMessage

private[spark] case class DroppedFromCache(rddId: Int, partition: Int, host: String, size: Long = 0L)
  extends CacheTrackerMessage

private[spark] case class MemoryCacheLost(host: String) extends CacheTrackerMessage

private[spark] case class RegisterRDD(rddId: Int, numPartitions: Int) extends CacheTrackerMessage

private[spark] case class SlaveCacheStarted(host: String, size: Long) extends CacheTrackerMessage

private[spark] case object GetCacheStatus extends CacheTrackerMessage

private[spark] case object GetCacheLocations extends CacheTrackerMessage

private[spark] case object StopCacheTracker extends CacheTrackerMessage

private[spark] class CacheTrackerActor extends Actor with Logging {
  // TODO: Should probably store (String, CacheType) tuples
  private val locs = new HashMap[Int, Array[List[String]]]

  /**
   * A map from the slave's host name to its cache size.
   */
  private val slaveCapacity = new HashMap[String, Long]
  private val slaveUsage = new HashMap[String, Long]

  private def getCacheUsage(host: String): Long = slaveUsage.getOrElse(host, 0L)

  private def getCacheCapacity(host: String): Long = slaveCapacity.getOrElse(host, 0L)

  private def getCacheAvailable(host: String): Long = getCacheCapacity(host) - getCacheUsage(host)

  def receive = {
    case SlaveCacheStarted(host: String, size: Long) =>
      slaveCapacity.put(host, size)
      slaveUsage.put(host, 0)
      sender ! true

    case RegisterRDD(rddId: Int, numPartitions: Int) =>
      logInfo("Registering RDD " + rddId + " with " + numPartitions + " partitions")
      locs(rddId) = Array.fill[List[String]](numPartitions)(Nil)
      sender ! true

    case AddedToCache(rddId, partition, host, size) =>
      slaveUsage.put(host, getCacheUsage(host) + size)
      locs(rddId)(partition) = host :: locs(rddId)(partition)
      sender ! true

    case DroppedFromCache(rddId, partition, host, size) =>
      slaveUsage.put(host, getCacheUsage(host) - size)
      // Do a sanity check to make sure usage is greater than 0.
      locs(rddId)(partition) = locs(rddId)(partition).filterNot(_ == host)
      sender ! true

    case MemoryCacheLost(host) =>
      logInfo("Memory cache lost on " + host)
      for ((id, locations) <- locs) {
        for (i <- 0 until locations.length) {
          locations(i) = locations(i).filterNot(_ == host)
        }
      }
      sender ! true

    case GetCacheLocations =>
      logInfo("Asked for current cache locations")
      sender ! locs.map { case (rrdId, array) => (rrdId -> array.clone()) }

    case GetCacheStatus =>
      val status = slaveCapacity.map { case (host, capacity) =>
        (host, capacity, getCacheUsage(host))
      }.toSeq
      sender ! status

    case StopCacheTracker =>
      logInfo("Stopping CacheTrackerActor")
      sender ! true
      context.stop(self)
  }
}

private[spark] class CacheTracker(actorSystem: ActorSystem, isMaster: Boolean, theCache: Cache)
  extends Logging {


  val registeredRddIds = new HashSet[Int]
  val cache = theCache.newKeySpace()

  val ip: String = System.getProperty("spark.master.host", "localhost")
  val port: Int = System.getProperty("spark.master.port", "7077").toInt
  val actorName: String = "CacheTracker"

  implicit val timeout: akka.util.Timeout = 10 seconds

  var trackerActor: ActorRef = if (isMaster) {
    actorSystem.actorOf(Props(new CacheTrackerActor))
  } else {
    val url = "akka://spark@%s:%s/user/%s".format(ip, port, actorName)
    Await.result(actorSystem.actorSelection(url).resolveOne(), timeout.duration)
  }

  val loading = new HashSet[(Int, Int)]

  def askTracker(message: Any): Any = {
    try {
      val future = trackerActor.ask(message)(timeout)
      Await.result(future, timeout.duration)
    } catch {
      case e: Exception =>
        throw new SparkException("Error communicating with CacheTracker")
    }
  }

  def communicate(message: Any) {
    if (askTracker(message) != true) {
      throw new SparkException("Error reply received from CacheTracker")
    }
  }

  def registerRDD(rddId: Int, numPartitions: Int) {
    registeredRddIds.synchronized {
      if (!registeredRddIds.contains(rddId)) {
        logInfo("Registering RDD ID " + rddId + " with cache")
        registeredRddIds += rddId
        communicate(RegisterRDD(rddId, numPartitions))
      }
    }
  }

  def cacheLost(host: String) {
    communicate(MemoryCacheLost(host))
    logInfo("CacheTracker successfully removed entries on " + host)
  }

  def getCacheStatus(): Seq[(String, Long, Long)] = {
    askTracker(GetCacheStatus).asInstanceOf[Seq[(String, Long, Long)]]
  }

  def getLocationsSnapshot(): HashMap[Int, Array[List[String]]] = {
    askTracker(GetCacheLocations).asInstanceOf[HashMap[Int, Array[List[String]]]]
  }


  def getOrCompute[T](rdd: RDD[T], split: Split)(implicit m: ClassManifest[T]): Iterator[T] = {
    logInfo("Looking for RDD partition %d:%d".format(rdd.id, split.index))
    val cachedVal = cache.get(rdd.id, split.index)
    if (cachedVal != null) {
      // Split is in cache, so just return its values
      logInfo("Found partition in cache!")
      cachedVal.asInstanceOf[Array[T]].iterator
    } else {
      // Mark the split as loading (unless someone else marks it first)
      val key = (rdd.id, split.index)
      loading.synchronized {
        while (loading.contains(key)) {
          // Someone else is loading it; let's wait for them
          try {
            loading.wait()
          } catch {
            case _ =>
          }
        }
        // See whether someone else has successfully loaded it. The main way this would fail
        // is for the RDD-level cache eviction policy if someone else has loaded the same RDD
        // partition but we didn't want to make space for it. However, that case is unlikely
        // because it's unlikely that two threads would work on the same RDD partition. One
        // downside of the current code is that threads wait serially if this does happen.
        val cachedVal = cache.get(rdd.id, split.index)
        if (cachedVal != null) {
          return cachedVal.asInstanceOf[Array[T]].iterator
        }
        // Nobody's loading it and it's not in the cache; let's load it ourselves
        loading.add(key)
      }
      // If we got here, we have to load the split
      // Tell the master that we're doing so

      // TODO: fetch any remote copy of the split that may be available
      logInfo("Computing partition " + split)
      var array: Array[T] = null
      var putResponse: CachePutResponse = null
      try {
        array = rdd.compute(split).toArray(m)
        putResponse = cache.put(rdd.id, split.index, array)
      } finally {
        // Tell other threads that we've finished our attempt to load the key (whether or not
        // we've actually succeeded to put it in the map)
        loading.synchronized {
          loading.remove(key)
          loading.notifyAll()
        }
      }

      putResponse match {
        case CachePutSuccess(size) => {
          // Tell the master that we added the entry. Don't return until it
          // replies so it can properly schedule future tasks that use this RDD.

          askTracker(AddedToCache(rdd.id, split.index, Utils.getHost, size))
        }
        case _ => null
      }
      array.iterator
    }
  }

  def dropEntry(datasetId: Any, partition: Int) {
    val (keySpaceId, innerId) = datasetId.asInstanceOf[(Any, Any)]
    if (keySpaceId == cache.keySpaceId) {
      trackerActor ! DroppedFromCache(innerId.asInstanceOf[Int], partition, Utils.getHost)
    }
  }

  def stop(): Unit = {
    askTracker(StopCacheTracker)
    registeredRddIds.clear()
    trackerActor = null
  }
}