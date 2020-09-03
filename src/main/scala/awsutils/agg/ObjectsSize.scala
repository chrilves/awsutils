package awsutils
package agg

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import awsutils.io.FS
import awsutils.lib.Dates
import cats.effect.IO
import software.amazon.awssdk.services.s3.model.S3Object
import cats.syntax.traverse._
import cats.instances.list._

/** Used to count the number of files and total size of S3 prefix.
  *
 * @param objects the number of S3 objects
  * @param size the total size of these objets
  */
final case class ObjectsSize(objects: Long, size: Long) {
  @inline def +(i: ObjectsSize): ObjectsSize = ObjectsSize(objects + i.objects, size + i.size)
}
object ObjectsSize {
  /* The neutral element: {{{ zero + info = info }}}  */
  val empty: ObjectsSize = ObjectsSize(0, 0)
}

object Agg {

  /** Used to count for each date, the number of files and total size of data for this date */
  type Aggregate = Map[LocalDate, ObjectsSize]

  /** Update the aggregate with a new S3 object */
  def update(agg: Aggregate, oi: S3Object): Aggregate = {
    val pat = ".*/dt=([0-9]{4}-[0-9]{2}-[0-9]{2})/.*parquet".r
    oi.key() match {
      case pat(sdt) =>
        val dt = LocalDate.parse(sdt)
        agg + (dt -> (agg.getOrElse(dt, ObjectsSize.empty) + ObjectsSize(1, oi.size())))
      case _ =>
        agg
    }
  }

  /** Used to print the aggregate as a CSV file */
  def printAggregate(file: String)(agg: Aggregate): IO[Unit] = {
    FS.withFile(file).use { println =>
      for {
        _ <- println("date,objects,size")
        _ <- Dates.range(agg.keySet).toList.traverse { (dt: LocalDate) =>
          val info = agg.getOrElse(dt, ObjectsSize.empty)
          println(s"${dt.format(DateTimeFormatter.ISO_LOCAL_DATE)},${info.objects.toString},${info.size.toString}")
        }
      } yield ()
    }
  }
}
