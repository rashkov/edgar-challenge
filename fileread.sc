import java.text.SimpleDateFormat
import java.util.Date
import java.io._

import scala.io.Source

case class Session(ip: String,
                   first_req: Long,
                   last_req: Long,
                   num_requests: Int)

def parse_date(date_str: String): Date = {
  val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  formatter.parse(date_str, new java.text.ParsePosition(0))
}

def get_completed_sessions(data: Map[String, Session], current_time: Long, inactivity_period: Int): Map[String, Session] ={
  data.filter((t) => (current_time - t._2.last_req)/1000 >= inactivity_period)
}

def get_ongoing_sessions(data: Map[String, Session], current_time: Long, inactivity_period: Int): Map[String, Session] ={
  data.filter((t) => (current_time - t._2.last_req)/1000 < inactivity_period)
}

def write_completed_sessions(completed_sessions: Map[String, Session]) = {
  for(session <- completed_sessions){
    println(s"Completed session ${session._1}")
  }
}

val inactivity_filename = "/path/to/input/inactivity_period.txt"
val inactivity_period: Int = Source.fromFile(inactivity_filename).getLines().toList(0).toInt

val output_filename = "/path/to/output/sessionization.txt"
val output_writer = new BufferedWriter(new FileWriter(new File(output_filename)))

val filename =
  "/path/to/input/log.csv"
var ongoing_sessions = Map[String, Session]()
for ((line, i) <- Source.fromFile(filename).getLines.zipWithIndex) if (i>0) {
  val elements = line.split(",")
  val ip = elements(0)
  val date = elements(1)
  val time = elements(2)
  val date_str = s"${date} ${time}"
  val current_line_epoch_time: Long = parse_date(date_str).getTime

  val completed_sessions = get_completed_sessions(ongoing_sessions, current_line_epoch_time, inactivity_period)
  write_completed_sessions(completed_sessions)
  ongoing_sessions = ongoing_sessions -- completed_sessions.keys
  //ongoing_sessions = get_ongoing_sessions(ongoing_sessions, current_line_epoch_time, inactivity_period)

  if (ongoing_sessions contains ip) {
    val session = ongoing_sessions(ip)
    val seconds_since = (current_line_epoch_time  - session.last_req)/1000
    val new_session = session.copy(num_requests = session.num_requests+1, last_req = current_line_epoch_time )
    println(s"Second: ${ip} ${seconds_since}")
    //output_writer.write(s"Second: ${ip} ${seconds_since}\n")
    ongoing_sessions = ongoing_sessions.updated(ip, new_session)
  } else {
    val session = Session(ip, current_line_epoch_time , current_line_epoch_time , 1)
    ongoing_sessions = ongoing_sessions.updated(ip, session)
  }
}
println(ongoing_sessions)

output_writer.close
