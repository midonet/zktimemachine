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
import java.nio.ByteBuffer;

import java.util.{HashMap,ArrayList}
import java.util.Date
import java.io.{File, FileFilter}
import java.io.BufferedInputStream
import java.io.FileInputStream

import org.apache.jute.BinaryInputArchive
import org.apache.jute.InputArchive
import org.apache.jute.Record
import java.util.zip.Adler32
import java.util.zip.CheckedInputStream

import org.apache.zookeeper.KeeperException.NoNodeException
import org.apache.zookeeper.server.persistence.FileSnap
import org.apache.zookeeper.server.persistence.{FileTxnLog,Util,FileTxnSnapLog}
import org.apache.zookeeper.txn._
import org.apache.zookeeper.ZooDefs.OpCode

import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}
import org.rogach.scallop._

object GrepOps {
    val timeFormatter = ISODateTimeFormat.dateTime

    def printTxn(txn: Record): String = {
        txn match {
            case t: CreateTxn =>
                val data = if (t.getData != null) { new String(t.getData) } else { "EMPTY" }
                s"CREATE(path=${t.getPath}, data=${data}, ephemeral=${t.getEphemeral})"
            case t: DeleteTxn =>
                s"DELETE(path=${t.getPath})"
            case t: SetDataTxn =>
                val data = if (t.getData != null) { new String(t.getData) } else { "EMPTY" }
                s"SET_DATA(path=${t.getPath}, data=${data}, version=${t.getVersion})"
            case t: CheckVersionTxn =>
                s"CHECK_VERSION(path=${t.getPath}, version=${t.getVersion})"
            case t: CreateSessionTxn =>
                s"CREATE_SESSION(timeout=${t.getTimeOut})"
            case t: ErrorTxn =>
                s"ERROR(err=${t.getErr})"
            case t: MultiTxn => printMulti(t)
            case t: SetACLTxn =>
                s"SET_ACL(path=${t.getPath})"
            case t: SetMaxChildrenTxn =>
                s"SET_MAX_CHILDREN(path=${t.getPath}, max=${t.getMax})"
            case _ =>
                s"UNKNOWN(${txn})"
        }
    }

    def decodeMulti(multi: MultiTxn): List[Record] = {
        multi.getTxns.asScala.toList.flatMap(
            t => {
                val record = t.getType match {
                    case OpCode.create => Some(new CreateTxn())
                    case OpCode.delete => Some(new DeleteTxn())
                    case OpCode.setData => Some(new SetDataTxn())
                    case OpCode.error => Some(new ErrorTxn())
                    case OpCode.check => Some(new CheckVersionTxn())
                    case _ => None
                }
                record match {
                    case Some(r) => {
                        val bb = ByteBuffer.wrap(t.getData())
                        ByteBufferInputStream.byteBuffer2Record(bb, r)
                        Some(r)
                    }
                    case None => {
                        System.err.println(s"\t-\tCOULD NOT DECODE(${t})")
                        None
                    }
                }
            })
    }

    def printMulti(multi: MultiTxn): String = {
        val buf = new StringBuffer(s"MULTI (${multi.getTxns.size} ops)\n")
        for (t <- decodeMulti(multi)) {
            buf.append(s"\t-\t${printTxn(t)}\n")
        }
        buf.toString
    }

    def printTxn(hdr: TxnHeader, txn: Record): Unit = {
        println(s"${timeFormatter.print(hdr.getTime)}, s:0x${java.lang.Long.toHexString(hdr.getClientId)}, zx:0x${java.lang.Long.toHexString(hdr.getZxid)} ${printTxn(txn)}")
    }

    def getPath(txn: Record): String = {
        txn match {
            case t: CreateTxn => t.getPath
            case t: DeleteTxn => t.getPath
            case t: SetDataTxn => t.getPath
            case t: CheckVersionTxn => t.getPath
            case t: SetACLTxn => t.getPath
            case t: SetMaxChildrenTxn => t.getPath
            case t: MultiTxn => decodeMulti(t).map(t => { getPath(t) }).mkString
            case _ => ""
        }
    }

    def main(args: Array[String]) {
        val opts = new ScallopConf(args) {
            val startTime = opt[String](
                "start-time",
                descr = s"Time to search from, in ISO8601 format",
                default = Some("1970-01-01T00:00:00.000+00:00"))
            val endTime = opt[String](
                "end-time",
                descr = s"Time to search to, in ISO8601 format",
                default = Some(timeFormatter.print(new DateTime())))

            val session = opt[String]("session",
                                      descr = "Filter by session")
            val path = opt[String]("path",
                                   descr = "Filter by znode path")
            val logDir = opt[String]("zk-txn-log-dir",
                                     descr = "ZooKeeper transaction log directory",
                                     required = true)
            val help = opt[Boolean]("help", noshort = true,
                                    descr = "Show this message")
        }

        val directory = new File(opts.logDir.get.get)
        val znode = args(1)
        val startTime = timeFormatter.parseDateTime(opts.startTime.get.get)
        val endTime = timeFormatter.parseDateTime(opts.endTime.get.get)

        val startZxid: Long = TimeMachine.lastLogZxidBeforeTimestamp(
            directory, startTime.getMillis).getOrElse(0)

        val txnLog = new FileTxnLog(directory)
        val iter = txnLog.read(startZxid)
        val nodes = new ArrayList[String]
        val creations = new HashMap[String,Long]
        val deletions = new HashMap[String,Long]

        val sessionId: Long = opts.session.get match {
            case Some(idstr) => java.lang.Long.decode(idstr)
            case None => -1
        }

        while (iter.getHeader != null
                   && iter.getHeader.getTime < startTime.getMillis) {
            iter.next
        }

        while (iter.getHeader != null
                   && iter.getHeader.getTime >= startTime.getMillis
                   && iter.getHeader.getTime <= endTime.getMillis) {
            val hdr = iter.getHeader
            val txn = iter.getTxn

            var skip = false
            if (opts.session.supplied) {
                skip |= !(hdr.getClientId == sessionId)
            }

            if (opts.path.supplied) {
                skip |= !(getPath(txn).contains(opts.path.get.get))
            }

            if (!skip) {
                printTxn(hdr, txn);
            }

            // if (txn.isInstanceOf[CreateTxn]) {
            //     val create = txn.asInstanceOf[CreateTxn]
            //     if (create.getPath.startsWith(znode)) {
            //         nodes.add(create.getPath)
            //         creations.put(create.getPath, hdr.getTime)
            //     }
            // } else if (txn.isInstanceOf[DeleteTxn]) {
            //     val delete = txn.asInstanceOf[DeleteTxn]
            //     if (delete.getPath.startsWith(znode)) {
            //         deletions.put(delete.getPath, hdr.getTime)
            //     }
            // }

            iter.next()
        }
    }
}
