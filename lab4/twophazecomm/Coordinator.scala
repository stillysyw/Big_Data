package twoPhaze


import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooDefs, ZooKeeper}

import java.util.concurrent.TimeUnit

case class Coordinator(hostPort:String,
                       root:String,
                       n_workers:Integer) extends Watcher {
  val zookeeper = new ZooKeeper(hostPort, 3000, this)
  val coordinatorPath = root + "/coordinator"


  if (zookeeper == null) throw new Exception("ZK is NULL.")
  override def process(event: WatchedEvent): Unit = {
    
  }

  def run(): Unit = {
    zookeeper.create(coordinatorPath, Array.emptyByteArray, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)

    while(true) {
      val workers = zookeeper.getChildren(coordinatorPath, this)
      if (workers.size == n_workers) {
        println("All clients voted")
        var commits = 0
        var aborts = 0
        for (i <- 0 until n_workers) {
          val w = workers.get(i)
          val data = new String(zookeeper.getData(s"$coordinatorPath/client_$i", false, null))
          if (data == "commit") commits += 1
          else if (data == "abort") aborts += 1
        }
        val decision = if (commits > aborts) "commit" else "abort"
        for (i <- 0 until n_workers) {
          val w = workers.get(i)
          zookeeper.setData(s"$coordinatorPath/client_$i", decision.getBytes, -1)
        }
        println(decision)
        while (true) {
          val workers = zookeeper.getChildren(coordinatorPath, this)
          if (workers.size == 0) {
            zookeeper.delete(coordinatorPath, -1)
            zookeeper.close()
            return
          } else {
            TimeUnit.SECONDS.sleep(5)
          }
        }
      } else {
        println(s"register nodes $workers")
      }
    }
  }

}
