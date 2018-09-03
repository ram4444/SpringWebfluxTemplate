package main.kotlin.util

import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

@Component
class ScheduleTasks {

    private val logger = LoggerFactory.getLogger(ScheduleTasks::class.java)

    /** Example
     * This @Schedule annotation run every 5 seconds in this case. It can also
     * take a cron like syntax.
     * See https://docs.spring.io/spring/docs/current/javadoc-api/org/springframework/scheduling/support/CronSequenceGenerator.html
     * "0 0 * * * *" = the top of every hour of every day.
        * "10 * * * * *" = every ten seconds.
        * "0 0 8-10 * * *" = 8, 9 and 10 o'clock of every day.
        * "0 0 6,19 * * *" = 6:00 AM and 7:00 PM every day.
        * "0 0/30 8-10 * * *" = 8:00, 8:30, 9:00, 9:30, 10:00 and 10:30 every day.
        * "0 0 9-17 * * MON-FRI" = on the hour nine-to-five weekdays
        * "0 0 0 25 12 ?" = every Christmas Day at midnight
     */
    //@Scheduled(fixedRate = 5000)
    fun reportTime(){
        logger.info("The time is now ${DateTimeFormatter.ISO_LOCAL_TIME.format(LocalDateTime.now())}")
    }
}