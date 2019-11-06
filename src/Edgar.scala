import java.text.SimpleDateFormat
import java.util.Date
import java.io._

import Edgar.getClass

import scala.io.{BufferedSource, Source}

case class Session(ip: String,
                   first_req: Long,
                   last_req: Long,
                   num_requests: Int,
                   sort_order: Int)

object Edgar extends App {
  def parse_date(date_str: String): Date = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    formatter.parse(date_str, new java.text.ParsePosition(0))
  }

  def get_completed_sessions(data: Map[String, Session],
                             current_time: Long,
                             inactivity_period: Int): Map[String, Session] = {
    data.filter(
      (t) => (current_time - t._2.last_req) / 1000 > inactivity_period
    )
  }

  def write_completed_sessions(completed_sessions: Map[String, Session],
                               output_writer: BufferedWriter) = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val sessions_sorted =
      completed_sessions.toList.sortBy((t) => t._2.sort_order)
    for (session <- sessions_sorted) {
      val first_req = session._2.first_req
      val last_req = session._2.last_req
      val first_req_formatted_str = format.format(first_req)
      val last_req_formatted_str = format.format(last_req)
      val session_length_seconds = (last_req - first_req) / 1000 + 1
      val str =
        s"${session._2.ip},${first_req_formatted_str},${last_req_formatted_str},${session_length_seconds},${session._2.num_requests}\n"
      print(str)
      output_writer.write(str)
    }
  }

  val inactivity_period: Int = {
    val inactivity_file_handler =
      Source.fromInputStream(
        getClass.getResourceAsStream("/inactivity_period.txt")
      )
    val inactivity_period = inactivity_file_handler.getLines().toList.head.toInt
    inactivity_file_handler.close
    inactivity_period
  }

  val output_writer: BufferedWriter = {
    val output_filename = "sessionization.txt"
    new BufferedWriter(new FileWriter(new File(output_filename)))
  }

  val log_file_handler: BufferedSource = {
    Source.fromInputStream(getClass.getResourceAsStream("/log.csv"))
  }

  var ongoing_sessions = Map[String, Session]()
  for ((line, i) <- log_file_handler.getLines.zipWithIndex) if (i > 0) {
    val elements = line.split(",")
    val ip = elements(0)
    val date = elements(1)
    val time = elements(2)
    val date_str = s"${date} ${time}"
    val current_line_epoch_time: Long = parse_date(date_str).getTime

    // Add the new line to our data-set
    if (ongoing_sessions contains ip) {
      val session = ongoing_sessions(ip)
      val new_session = session.copy(
        num_requests = session.num_requests + 1,
        last_req = current_line_epoch_time
      )
      ongoing_sessions = ongoing_sessions.updated(ip, new_session)
    } else {
      val session =
        Session(ip, current_line_epoch_time, current_line_epoch_time, 1, i)
      ongoing_sessions = ongoing_sessions.updated(ip, session)
    }

    // Process completed sessions and update ongoing sessions
    val completed_sessions = get_completed_sessions(
      ongoing_sessions,
      current_line_epoch_time,
      inactivity_period
    )
    write_completed_sessions(completed_sessions, output_writer)
    ongoing_sessions = ongoing_sessions -- completed_sessions.keys
  }
  // Close out any ongoing sessions
  write_completed_sessions(ongoing_sessions, output_writer)

  // Close any open files
  log_file_handler.close
  output_writer.close
}
