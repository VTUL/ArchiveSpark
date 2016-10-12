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

package de.l3s.archivespark.specific.books

import de.l3s.archivespark.dataspecs.{DataSpec, TextFileDataLoader}

class IATxtBooksHdfsSpec private(metaPath: String) extends DataSpec[(String, String), TxtBookRecord] with TextFileDataLoader {
  val DetailsUrlMetaField = "identifier-access"

  override def dataPath: String = metaPath

  override def parse(data: (String, String)): Option[TxtBookRecord] = {
    val (filename, text) = data
    BookMetaData.fromXml(text).flatMap { meta =>
      meta.raw.getOrElse(DetailsUrlMetaField, Seq()).headOption.map { url =>
        new TxtBookRecord(meta, new IATxtBookAccessor(url))
      }
    }
  }
}

object IATxtBooksHdfsSpec {
  def apply(metaPath: String): IATxtBooksHdfsSpec = new IATxtBooksHdfsSpec(metaPath)
}