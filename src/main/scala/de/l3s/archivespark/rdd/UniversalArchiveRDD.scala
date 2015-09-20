package de.l3s.archivespark.rdd

import de.l3s.archivespark.cdx.CdxRecord
import de.l3s.archivespark.records.UniversalArchiveRecord
import de.l3s.archivespark.{ArchiveRDD, ArchiveSpark}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object UniversalArchiveRDD {
  def apply(cdxPath: String)(implicit sc: SparkContext) = new UniversalArchiveRDD(ArchiveSpark.cdx(cdxPath))
}

class UniversalArchiveRDD private (parent: RDD[CdxRecord]) extends ArchiveRDD[CdxRecord](parent) {
  override protected def record(cdx: CdxRecord) = new UniversalArchiveRecord(cdx)
}