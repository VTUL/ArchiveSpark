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

package de.l3s.archivespark.enrich.functions

import de.l3s.archivespark.enrich.{EnrichFunc, _}
import de.l3s.archivespark.enrich.dataloads.TextLoad

class Prefix private (characters: Int) extends DefaultFieldDependentEnrichFunc[EnrichRoot with TextLoad, String, String] with SingleField[String] {
  override def dependency: EnrichFunc[EnrichRoot with TextLoad, _] = DataLoad(TextLoad.Field)
  override def dependencyField: String = TextLoad.Field

  override def resultField = "prefix-" + characters
  override def aliases: Map[String, String] = Map("prefix" -> resultField)

  override protected[enrich] def derive(source: TypedEnrichable[String], derivatives: Derivatives): Unit = {
    derivatives << source.get.take(characters)
  }
}

object Prefix extends Prefix(1000) {
  def apply(characters: Int): Prefix = new Prefix(characters)
}