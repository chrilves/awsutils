package object awsutils {
  @SuppressWarnings(Array("org.wartremover.warts.Equals", "org.wartremover.warts.Null"))
  implicit final class AnyOps[A](self: A) {

    /** Same type equality */
    @inline def ===(other: A): Boolean = self == other

    /** Same type inequality */
    @inline def =/=(other: A): Boolean = self != other

    @inline def |>[B](f: A => B): B = f(self)

    /** When opt is defined, transform self by f, otherwise keep self untouched. */
    @inline def when[B](opt: Option[B], f: (A, B) => A): A =
      opt match {
        case Some(b) => f(self, b)
        case _       => self
      }

    @inline def isNull: Boolean  = self == null
    @inline def nonNull: Boolean = self != null
  }
}
