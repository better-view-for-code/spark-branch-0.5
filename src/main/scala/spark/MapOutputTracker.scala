package spark

import java.util.concurrent.ConcurrentHashMap

import akka.actor._
import akka.pattern.ask

import scala.collection.immutable.HashMap
import scala.collection.mutable.HashSet
import scala.concurrent.Await
import scala.concurrent.duration._

private[spark] sealed trait MapOutputTrackerMessage

private[spark] case class GetMapOutputStatuses(shuffleId: Int, requester: String)
  extends MapOutputTrackerMessage

private[spark] case object StopMapOutputTracker extends MapOutputTrackerMessage

private[spark] class MapOutputTrackerActor(tracker: MapOutputTracker) extends Actor with Logging {
  def receive = {
    case GetMapOutputStatuses(shuffleId: Int, requester: String) =>
      logInfo("Asked to send map output locations for shuffle " + shuffleId + " to " + requester)
      sender ! tracker.getSerializedLocations(shuffleId)

    case StopMapOutputTracker =>
      logInfo("MapOutputTrackerActor stopped!")
      sender ! true
      context.stop(self)
  }
}

private[spark] class MapOutputTracker(actorSystem: ActorSystem, isMaster: Boolean) extends Logging {
  val ip: String = System.getProperty("spark.master.host", "localhost")
  val port: Int = System.getProperty("spark.master.port", "7077").toInt
  val actorName: String = "MapOutputTracker"

  implicit val timeout: akka.util.Timeout = 10 seconds

  // Incremented every time a fetch fails so that client nodes know to clear
  // their cache of map output locations if this happens.
  private var generation: Long = 0
  private val generationLock = new java.lang.Object

  // Cache a serialized version of the output statuses for each shuffle to send them out faster
  var cacheGeneration = generation
  val cachedSerializedStatuses = new HashMap[Int, Array[Byte]]

  private var serverUris = new ConcurrentHashMap[Int, Array[String]]

  var trackerActor: ActorRef = if (isMaster) {
    val actor = actorSystem.actorOf(Props(new MapOutputTrackerActor(this)), name = actorName)
    logInfo("Registered MapOutputTrackerActor actor")
    actor
  } else {
    val url = "akka://spark@%s:%s/user/%s".format(ip, port, actorName)
    Await.result(actorSystem.actorSelection(url).resolveOne(), timeout.duration)
  }

  // Send a message to the trackerActor and get its result within a default timeout, or
  // throw a SparkException if this fails.
  def askTracker(message: Any): Any = {
    try {
      val future = trackerActor.ask(message)(timeout)
      Await.result(future, timeout.duration)
    } catch {
      case e: Exception =>
        throw new SparkException("Error communicating with MapOutputTracker")
    }
  }

  // Send a one-way message to the trackerActor, to which we expect it to reply with true.
  def communicate(message: Any) {
    if (askTracker(message) != true) {
      throw new SparkException("Error reply received from MapOutputTracker")
    }
  }

  def registerShuffle(shuffleId: Int, numMaps: Int) {
    if (serverUris.get(shuffleId) != null) {
      throw new IllegalArgumentException("Shuffle ID " + shuffleId + " registered twice")
    }
    serverUris.put(shuffleId, new Array[String](numMaps))
  }

  def registerMapOutput(shuffleId: Int, mapId: Int, serverUri: String) {
    val array = serverUris.get(shuffleId)
    array.synchronized {
      array(mapId) = serverUri
    }
  }

  def registerMapOutputs(shuffleId: Int, locs: Array[String]) {
    serverUris.put(shuffleId, Array[String]() ++ locs)
  }

  def unregisterMapOutput(shuffleId: Int, mapId: Int, serverUri: String) {
    val array = serverUris.get(shuffleId)
    if (array != null) {
      array.synchronized {
        if (array(mapId) == serverUri) {
          array(mapId) = null
        }
      }
      incrementGeneration()
    } else {
      throw new SparkException("unregisterMapOutput called for nonexistent shuffle ID")
    }
  }

  // Remembers which map output locations are currently being fetched on a worker
  val fetching = new HashSet[Int]

  // Called on possibly remote nodes to get the server URIs and output sizes for a given shuffle
  def getServerUris(shuffleId: Int): Array[String] = {
    val statuses = serverUris.get(shuffleId)
    if (statuses == null) {
      logInfo("Don't have map outputs for shuffle " + shuffleId + ", fetching them")
      fetching.synchronized {
        if (fetching.contains(shuffleId)) {
          // Someone else is fetching it; wait for them to be done
          while (fetching.contains(shuffleId)) {
            try {
              fetching.wait()
            } catch {
              case e: InterruptedException =>
            }
          }
          return serverUris.get(shuffleId)
        } else {
          fetching += shuffleId
        }
      }
      // We won the race to fetch the output locs; do so
      logInfo("Doing the fetch; tracker actor = " + trackerActor)
      val host = System.getProperty("spark.hostname", Utils.localHostName)
      // This try-finally prevents hangs due to timeouts:

      val fetchedBytes =
        askTracker(GetMapOutputStatuses(shuffleId, host)).asInstanceOf[Array[String]]
      //fetchedStatuses = deserializeStatuses(fetchedBytes)
      logInfo("Got the output locations")

      serverUris.put(shuffleId, fetchedBytes)

      fetching.synchronized {
        fetching -= shuffleId
        fetching.notifyAll()
      }

      fetchedBytes
    } else {
      statuses
    }
  }

  def stop() {
    communicate(StopMapOutputTracker)
    serverUris.clear()
    trackerActor = null
  }

  // Called on master to increment the generation number
  def incrementGeneration() {
    generationLock.synchronized {
      generation += 1
      logDebug("Increasing generation to " + generation)
    }
  }

  // Called on master or workers to get current generation number
  def getGeneration: Long = {
    generationLock.synchronized {
      return generation
    }
  }

  // Called on workers to update the generation number, potentially clearing old outputs
  // because of a fetch failure. (Each Mesos task calls this with the latest generation
  // number on the master at the time it was created.)
  def updateGeneration(newGen: Long) {
    generationLock.synchronized {
      if (newGen > generation) {
        logInfo("Updating generation to " + newGen + " and clearing cache")
        serverUris = new ConcurrentHashMap[Int, Array[String]]
        generation = newGen
      }
    }
  }

  def getSerializedLocations(shuffleId: Int): Array[String] = {
    serverUris.get(shuffleId)
  }
}