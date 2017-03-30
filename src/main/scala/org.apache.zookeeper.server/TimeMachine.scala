/*
 * Copyright 2015 Midokura SARL
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zookeeper.server

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import java.util.HashMap
import java.util.Date
import java.util.regex.Pattern
import java.io.{File, FileFilter, FilenameFilter}
import java.io.BufferedInputStream
import java.io.FileInputStream


import org.apache.jute.BinaryInputArchive
import org.apache.jute.InputArchive
import java.util.zip.Adler32
import java.util.zip.CheckedInputStream

import org.apache.zookeeper.KeeperException.NoNodeException
import org.apache.zookeeper.server.persistence.FileSnap
import org.apache.zookeeper.server.persistence.{FileTxnLog,Util,FileTxnSnapLog}

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.ISODateTimeFormat
import org.rogach.scallop._

object TimeMachine {
    val timeFormatter = ISODateTimeFormat.dateTime

    def lastLogZxidBeforeTimestamp(logDir: File, timestamp: Long)
            : Option[Long] = {
        val logPattern = Pattern.compile("log\\.([0-9a-f]*)")
        val logs = logDir.listFiles(
            new FilenameFilter() {
                def accept(dir: File, name: String): Boolean = {
                    logPattern.matcher(name).matches
                }
            })
        val zkTxnLog = new FileTxnLog(logDir)
        val fileTimes = logs.flatMap(
            f => {
                val m = logPattern.matcher(f.getName)
                assert(m.matches)
                val zxid = java.lang.Long.parseLong(m.group(1), 16)
                val hdr = zkTxnLog.read(zxid).getHeader()
                if (hdr != null) {
                    Some((hdr.getTime(), zxid))
                } else {
                    None
                }
            }).sortBy(_._1)

        for (f <- fileTimes) {
            println(s"Log(${f._2.toHexString}) started at ${timeFormatter.print(f._1)}")
        }

        fileTimes.filter(_._1 <= timestamp).lastOption match {
            case Some(x) => Some(x._2)
            case None => None
        }
    }

    def lastSnapshotBeforeZxid(snapshotDir: File, zxid: Long)
            : Option[File] = {
        val snapshotPattern = Pattern.compile("snapshot\\.([0-9a-f]*)")
        val snapshots = snapshotDir.listFiles(
            new FilenameFilter() {
                def accept(dir: File, name: String): Boolean = {
                    snapshotPattern.matcher(name).matches
                }
            })
        snapshots.flatMap(
            f => {
                val m = snapshotPattern.matcher(f.getName)
                if (m.matches) {
                    val zxid = java.lang.Long.parseLong(m.group(1), 16)
                    Some((zxid, f))
                } else {
                    None
                }
            }
        ).sortBy(_._1)
            .filter(_._1 <= zxid)
            .lastOption match {
            case Some(x) => Some(x._2)
            case None => None
        }
    }

    def lastSnapshotBeforeTimestamp(snapshotDir: File,
                                    logDir: File, timestamp: Long)
            : Option[File] = {
        lastLogZxidBeforeTimestamp(logDir, timestamp) match {
            case Some(zxid) => lastSnapshotBeforeZxid(snapshotDir, zxid)
            case None => None
        }
    }

    def main(args: Array[String]) {
        val opts = new ScallopConf(args) {
            val queryTime = opt[String](
                "time",
                descr = s"Time to dump snapshot at in ISO8601 format",
                default = Some(timeFormatter.print(new DateTime())))
            val snapshotDir = opt[String]("zk-snapshot-dir",
                                          descr = "ZooKeeper snapshot directory",
                                          required = true)
            val logDir = opt[String]("zk-txn-log-dir",
                                     descr = "ZooKeeper transaction log directory",
                                     default = snapshotDir.get)
            val nolog = opt[Boolean]("nolog", noshort = true,
                                     descr = "Don't look at logs")
            val help = opt[Boolean]("help", noshort = true,
                                    descr = "Show this message")
        }

        val queryTime = opts.queryTime.get match {
            case Some(date) => timeFormatter.parseDateTime(date)
            case None => new DateTime()
        }

        val logDir = new File(opts.logDir.get.get)
        val snapDir = new File(opts.snapshotDir.get.get)

        val snapFile = if (opts.nolog.get.get) {
            lastSnapshotBeforeZxid(snapDir, Long.MaxValue)
        } else {
            lastSnapshotBeforeTimestamp(
                snapDir, logDir, queryTime.getMillis)
        }
        if (!snapFile.isDefined) {
            println(s"No snapshot found before ${queryTime}")
            System.exit(1)
        }

        println(s"Replaying up until time ${queryTime}")
        val is = new CheckedInputStream(
            new BufferedInputStream(new FileInputStream(snapFile.get)),
            new Adler32())
        val ia = BinaryInputArchive.getArchive(is)
        val fileSnap = new FileSnap(null)


        val dataTree = new DataTree()
        val sessions = new HashMap[java.lang.Long, Integer]()

        fileSnap.deserialize(dataTree, sessions, ia);

        val txnSnapLog = new FileTxnSnapLog(snapDir, logDir)

        val zxid = Util.getZxidFromName(snapFile.get.getName, "snapshot")

        val txnlog = new FileTxnLog(logDir)
        val iter = txnlog.read(zxid)
        var i = 0
        var lastzxid = zxid
        var startTime = 0L
        var endTime = 0L
        while (iter.getHeader != null &&
                   iter.getHeader.getTime <= queryTime.getMillis) {
            val hdr = iter.getHeader
            val txn = iter.getTxn
            if (startTime == 0) {
                startTime = hdr.getTime
            }
            endTime = hdr.getTime
            try {
                txnSnapLog.processTransaction(hdr, dataTree, sessions, txn)
            } catch {
                case e: NoNodeException => println("No node for " + hdr)
            }
            i += 1
            lastzxid = hdr.getZxid
            iter.next()
        }
        val startDate = new Date(startTime)
        val endDate = new Date(endTime)
        println(s"Applied ${i} transactions from zxid ${zxid} (${startDate}) to ${lastzxid} (${endDate})")

        printZnode(dataTree, "/", 0, endTime)
    }

    def printZnode(dataTree: DataTree, name: String, depth: Int, endTime: Long) {
        //println("----")
        val n = dataTree.getNode(name)
        val children = n.getChildren()
        val numChildren = if (children != null) children.size() else 0
        var prefix = ""
        for (i <- 0 until depth) {
            prefix = prefix + "\t"
        }
        var data = 0;
        if (n.data != null) {
            data = n.data.length
        }
        val largenode = if (numChildren > 100) "LARGENODE" else ""
        val owner = n.stat.getEphemeralOwner
        val age = endTime - n.stat.getCtime()
        val creationTime = new Date(n.stat.getCtime())
        println(s"${prefix}- ${name} data:${data} children:${numChildren} createdBy:${owner} createdAt:${creationTime} age:${age} ${largenode} ")
        if (children != null) {
            for (child <- children.asScala) {
                printZnode(dataTree,
                           name + (if (name.equals("/")) ""
                                   else "/") + child,
                           depth + 1, endTime);
            }
        }
    }
}
