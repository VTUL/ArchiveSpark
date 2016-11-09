/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015-2016 Helge Holzmann (L3S) and Vinay Goel (Internet Archive)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package de.l3s.archivespark.jobs

import java.util.Calendar

import de.l3s.archivespark.ArchiveSpark
import de.l3s.archivespark.benchmarking.warcbase.{WarcBase, WarcRecord}
import de.l3s.archivespark.benchmarking.{Benchmark, BenchmarkLogger}
import de.l3s.archivespark.enrich.functions._
import de.l3s.archivespark.http.HttpResponse
import de.l3s.archivespark.implicits._
import de.l3s.archivespark.specific.warc.RawArchiveRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.warcbase.data.UrlUtils

object Benchmarking {
  val times = 10
  val retries = 1
  val logFile = "benchmarks.txt"
  val logValues = true

  val hbaseId = "HBase"
  val sparkId = "Spark"
  val archiveSparkId = "ArchiveSpark"

  val warcPath = "warc/example"
  val cdxPath = warcPath
  val hbaseTable = "example"

  def main(args: Array[String]): Unit = {
    val appName = "ArchiveSpark.Benchmarking"

    val conf = new SparkConf().setAppName(appName)
    ArchiveSpark.initialize(conf)
    conf.registerKryoClasses(Array(classOf[WarcRecord]))

    implicit val sc = new SparkContext(conf)
    implicit val logger = new BenchmarkLogger(logFile)

    Benchmark.retries = retries

    runOneUrl
    runOneDomain
    runOneMonthLatestOnline
    runOneDomainOnline
  }

  def archiveSpark(implicit sc: SparkContext) = ArchiveSpark.hdfs(s"$cdxPath/*.cdx", warcPath)

  def warcBase(implicit sc: SparkContext) = WarcBase.loadWarc(s"$warcPath/*.warc.gz").coalesce(ArchiveSpark.partitions(sc))

  def hbase(conf: Configuration => Unit)(implicit sc: SparkContext) = WarcBase.flatVersions(WarcBase.hbase(hbaseTable) { c => conf(c) }.repartition(ArchiveSpark.partitions(sc)))

  def rowKey(url: String) = Option(UrlUtils.urlToKey(url)).getOrElse(url)

  def benchmarkArchiveSpark(name: String)(rdd: => RDD[de.l3s.archivespark.specific.warc.WarcRecord])(implicit sc: SparkContext, logger: BenchmarkLogger) = {
    Benchmark.time(name, archiveSparkId, times) {
      val contentLength = rdd.mapEnrich(StringContent, "length") { content => content.length }
      (rdd.count, contentLength.filter(r => r("payload.string.length").isDefined).map(r => r.get[Int]("payload.string.length").get).sum)
    }.log(logValues)
  }

  def benchmarkSpark(name: String)(rdd: => RDD[WarcRecord])(implicit sc: SparkContext, logger: BenchmarkLogger) = {
    Benchmark.time(name, sparkId, times) {
      (rdd.count, rdd.map(r => r.getContentString.length).sum)
    }.log(logValues)
  }

  def benchmarkHbase(name: String)(rdd: => RDD[RawArchiveRecord])(implicit sc: SparkContext, logger: BenchmarkLogger) = {
    Benchmark.time(name, hbaseId, times) {
      (rdd.count, rdd.map(r => new String(r.httpResponse.payload).length).sum)
    }.log(logValues)
  }

  def runOneUrl(implicit sc: SparkContext, logger: BenchmarkLogger) = {
    val name = "one url"
    val url = "http://www.archive.org/services/collection-rss.php"

    benchmarkArchiveSpark(name) {
      archiveSpark.filter(r => r.originalUrl == url)
    }

    benchmarkSpark(name) {
      warcBase.filter(r => r.getUrl == url)
    }

    benchmarkHbase(name) {
      hbase { c =>
        c.set(TableInputFormat.SCAN_ROW_START, rowKey(url))
        c.set(TableInputFormat.SCAN_ROW_STOP, rowKey(url))
      }.map{case (timestamp, url, mime, record) => record}
    }
  }

  def runOneDomain(implicit sc: SparkContext, logger: BenchmarkLogger) = {
    val name = "one domain (text/html)"
    val domain = "archive.org"
    val reverse = domain.split("\\.").reverse.mkString(".")
    val next = reverse.substring(0, reverse.length - 1) + (reverse.charAt(reverse.length - 1) + 1).asInstanceOf[Char]

    benchmarkArchiveSpark(name) {
      archiveSpark
        .filter(r => r.mime == "text/html" && r.surtUrl.matches(s"(^|.*\\.)${domain + ".*$"}"))
    }

    benchmarkSpark(name) {
      warcBase
        .filter(r => r.getMimeType == "text/html" && r.getDomain.matches(s"(^|.*\\.)${domain + "$"}"))
    }

    benchmarkHbase(name) {
      hbase { c =>
        c.set(TableInputFormat.SCAN_COLUMNS, "c:text/html")
        c.set(TableInputFormat.SCAN_ROW_START, reverse)
        c.set(TableInputFormat.SCAN_ROW_STOP, next)
      }.filter{case (time, url, mime, record) => url.matches(s"^$reverse[\\.\\/].*")}
        .map{case (timestamp, url, mime, record) => record}
    }
  }

  def runOneMonthLatestOnline(implicit sc: SparkContext, logger: BenchmarkLogger) = {
    val name = "one month latest online"
    val year = 2008
    val month = 4
    val calendar = Calendar.getInstance()
    calendar.set(year, month - 1, 1, 0, 0, 0)
    val startDate = calendar.getTime
    calendar.set(year, month, 1, 0, 0, 0)
    val stopDate = calendar.getTime

    benchmarkArchiveSpark(name) {
      archiveSpark
        .filter(r => r.status == 200 && r.time.getYear == year && r.time.getMonthOfYear == month)
        .map(r => (r.surtUrl, r)).reduceByKey((r1, r2) => if (r1.time.compareTo(r2.time) > 0) r1 else r2, ArchiveSpark.partitions(sc))
        .values
    }

    benchmarkSpark(name) {
      warcBase
        .filter{r => HttpResponse(r.getContentBytes).statusLine.contains(" 200 ") && r.getCrawldate.startsWith(f"$year$month%02d")}
        .map(r => (r.getUrl, r))
        .reduceByKey({(r1, r2) => if (r1.getCrawldate.toInt > r2.getCrawldate.toInt) r1 else r2}, ArchiveSpark.partitions(sc))
        .values
    }

    benchmarkHbase(name) {
      hbase { c =>
        c.setLong(TableInputFormat.SCAN_TIMERANGE_START, startDate.getTime)
        c.setLong(TableInputFormat.SCAN_TIMERANGE_END, stopDate.getTime)
      }.filter{case (time, url, mime, record) => record.httpResponse.statusLine.contains(" 200 ")}
        .map{case (time, url, mime, record) => (url, (time, record))}
        .reduceByKey((tr1, tr2) => if (tr1._1 > tr2._1) tr1 else tr2, ArchiveSpark.partitions(sc))
        .map{case (url, tr) => tr._2}
    }
  }

  def runOneDomainOnline(implicit sc: SparkContext, logger: BenchmarkLogger) = {
    val name = "one domain (text/html) online"
    val domain = "archive.org"
    val reverse = domain.split("\\.").reverse.mkString(".")
    val next = reverse.substring(0, reverse.length - 1) + (reverse.charAt(reverse.length - 1) + 1).asInstanceOf[Char]

    benchmarkArchiveSpark(name) {
      archiveSpark
        .filter(r => r.mime == "text/html" && r.surtUrl.matches(s"(^|.*\\.)${domain + ".*$"}") && r.status == 200)
    }

    benchmarkSpark(name) {
      warcBase
        .filter(r => r.getMimeType == "text/html" && r.getDomain.matches(s"(^|.*\\.)${domain + "$"}") && HttpResponse(r.getContentBytes).statusLine.contains(" 200 "))
    }

    benchmarkHbase(name) {
      hbase { c =>
        c.set(TableInputFormat.SCAN_COLUMNS, "c:text/html")
        c.set(TableInputFormat.SCAN_ROW_START, reverse)
        c.set(TableInputFormat.SCAN_ROW_STOP, next)
      }.filter{case (time, url, mime, record) => url.matches(s"^$reverse[\\.\\/].*") && record.httpResponse.statusLine.contains(" 200 ")}
        .map{case (timestamp, url, mime, record) => record}
    }
  }
 }
