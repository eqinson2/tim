package com.ericsson.ema.tim.zookeeper

import java.io.IOException
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicLong

import com.ericsson.ema.tim.zookeeper.State.State
import com.ericsson.util.SystemPropertyUtil
import com.ericsson.zookeeper.ZooKeeperUtil
import org.apache.zookeeper.Watcher.Event
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}
import org.slf4j.LoggerFactory

/**
  * Created by eqinson on 2017/5/5.
  */
trait ZKConnectionChangeWatcher {
	def stateChange(state: State): Unit
}


class ZKConnectionManager {
	private val LOGGER = LoggerFactory.getLogger(classOf[ZKConnectionManager])

	private val SESSION_TIMEOUT = 6000
	private var connectStr: String = _
	private var zooKeeper: ZooKeeper = _
	private var listeners = List[ZKConnectionChangeWatcher]()

	@volatile
	private var waitForReconnect: Boolean = _
	private var reconnectExecutor: ExecutorService = _
	private var reconnFuture: Future[_] = _

	try
		connectStr = SystemPropertyUtil.getAndAssertProperty("com.ericsson.ema.tim.zkconnstr")
	catch {
		case e: IllegalArgumentException => connectStr = "localhost:6181"
	}

	def init(): Unit = {
		LOGGER.info("Start to init zookeeper connection manager.")
		connect()
		reconnectExecutor = Executors.newSingleThreadExecutor(new ZKNamedSequenceThreadFactory("ZKReconnect"))
		reconnFuture = reconnectExecutor.submit(new Runnable {
			override def run(): Unit = {
				while (!Thread.currentThread.isInterrupted) {
					try {
						Thread.sleep(60000)
						if (waitForReconnect && getConnection.isEmpty) {
							LOGGER.info("ZK reconnection is wanted.")
							connect()
						}
					} catch {
						case e: InterruptedException =>
							LOGGER.info("Interrupted from sleep, return directly.")
							return
						case e: Exception            =>
							LOGGER.error("Unexpected error happens", e)
					}

				}
			}
		})
	}

	private def connect(): Unit = {
		try {
			val watcher = new ConnectionWatcher
			zooKeeper = new ZooKeeper(connectStr, SESSION_TIMEOUT, watcher)
			watcher.waitUntilConnected()
		} catch {
			case e: IOException =>
				LOGGER.warn("Failed to create zookeeper connection.", e)
				zooKeeper = null
		}
	}

	def destroy(): Unit = {
		LOGGER.info("Start to destroy zookeeper connection manager.")
		reconnFuture.cancel(false)
		reconnectExecutor.shutdownNow
		try {
			if (!reconnectExecutor.awaitTermination(60, TimeUnit.SECONDS))
				LOGGER.warn("Failed to shutdown the reconnect monitor immediately.")
		}
		catch {
			case e: InterruptedException => LOGGER.warn("interrupted from await for termination")
		}
		getConnection.foreach(ZooKeeperUtil.closeNoException)
		zooKeeper = null
	}

	def getConnection: Option[ZooKeeper] = Option(zooKeeper)

	def registerListener(listener: ZKConnectionChangeWatcher): Unit = {
		listeners = listeners :+ listener
	}

	private def notifyListener(state: State): Unit = {
		listeners.foreach(_.stateChange(state))
	}

	private def getSessionId: String = getConnection.map(z => "0x" + java.lang.Long.toHexString(z.getSessionId)).getOrElse("NO-SESSION")

	private class ConnectionWatcher extends Watcher {
		final private val latch = new CountDownLatch(1)
		private var connectionHasBeenEstablished = false

		override def process(event: WatchedEvent): Unit = {
			event.getState match {
				case Event.KeeperState.Expired       =>
					LOGGER.error("The session [{}] in ZK has been expired will perform an automatic re-connection " + "attempt", getSessionId)
					connect()
					if (getConnection.isEmpty && !waitForReconnect) {
						LOGGER.error("Failed to reconnect the zookeeper server")
						waitForReconnect = true
					}
				case Event.KeeperState.SyncConnected =>
					LOGGER.info("Got connected event for session [{}] to zookeeper", getSessionId)
					if (connectionHasBeenEstablished)
						notifyListener(State.RECONNECTED)
					else
						notifyListener(State.CONNECTED)
					connectionHasBeenEstablished = true
					waitForReconnect = false
					latch.countDown()
				case Event.KeeperState.Disconnected  =>
					LOGGER.warn("The session [{}] in ZooKeeper has lost its connection", getSessionId)
					notifyListener(State.DISCONNECTED)
				case _                               => LOGGER.error("should not happen")
			}
		}

		def waitUntilConnected(): Unit = {
			try
				latch.await(60, TimeUnit.SECONDS)
			catch {
				case e: InterruptedException => LOGGER.trace(e.getMessage)
			}
		}
	}

}

object ZKConnectionManager {
	var instance: ZKConnectionManager = _

	def apply(): ZKConnectionManager = synchronized {
		if (instance == null)
			instance = new ZKConnectionManager
		instance
	}
}

object State extends Enumeration {
	type State = Value
	val CONNECTED, RECONNECTED, DISCONNECTED = Value
}


final private class ZKNamedSequenceThreadFactory(val threadName: String) extends ThreadFactory {
	private val counter = new AtomicLong(1L)

	override def newThread(runnable: Runnable) = new Thread(runnable, this.threadName + "-" + this.counter.getAndIncrement)
}


