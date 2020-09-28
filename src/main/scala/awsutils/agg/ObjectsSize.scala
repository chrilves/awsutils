package awsutils
package agg

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
