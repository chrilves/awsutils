package awsutils
package lib

import java.time.LocalDate

object Dates {

  /** Given a set of dates, returns the continuous interval containing all dates
    *
   * @param ks the set of dates
    * @return the interval [ks.min, ks.max] if ks is non empty, the empty list otherwise.
    */
  def range(ks: Set[LocalDate]): LazyList[LocalDate] =
    if (ks.isEmpty)
      LazyList.empty
    else {
      val end = ks.max
      LazyList.unfold(ks.min) { (dt: LocalDate) =>
        if (dt.isAfter(end))
          None
        else
          Some((dt, dt.plusDays(1)))
      }
    }

  /** Return all the dates between start and end (both included)
    * Start is always the fist element of the list and end always the last
    * (maybe the also the first if start == end).
    *
   * @param start the first element of the list
    * @param end   the last element of the list
    * @result all dates between start and end
    */
  def between(start: LocalDate, end: LocalDate): LazyList[LocalDate] =
    if (!start.isAfter(end) && !start.isBefore(end))
      LazyList(start)
    else {
      val (cond, next) =
        if (start.isBefore(end))
          ((_: LocalDate).isAfter(end), (_: LocalDate).plusDays(1))
        else
          ((_: LocalDate).isBefore(end), (_: LocalDate).minusDays(1))

      LazyList.unfold(start) { (dt: LocalDate) => if (cond(dt)) None else Some((dt, next(dt))) }
    }
}
