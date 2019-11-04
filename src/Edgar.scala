import scala.io.Source

object Edgar extends App{
  val filename = "/path/to/input/log.csv"
  for (line, i <- Source.fromFile(filename).getLines) {
    println(i)
  }

}
