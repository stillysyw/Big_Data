package twoPhaze

import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooDefs, ZooKeeper}

import java.util.concurrent.TimeUnit
import scala.util.Random

case class Client(
                   id:Integer,
                   hostPort:String,
                   root:String) extends Watcher {
  val zookeeper = new ZooKeeper(hostPort, 3000, this)
  val clientPath = root + "/client_" + id.toString

  override def process(event: WatchedEvent): Unit = {
  }

  def run(): Unit = {
    val value = if (Random.nextDouble() > 0.5) "commit" else "abort"
    while (zookeeper.exists(root, false) == null) {
      TimeUnit.SECONDS.sleep(5)
    }
    println(s"Node $id vote $value")
    zookeeper.create(clientPath, value.getBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
    TimeUnit.SECONDS.sleep(10)
    zookeeper.getData(clientPath, this, null)
    zookeeper.delete(clientPath, -1)
    zookeeper.close()
  }

}
