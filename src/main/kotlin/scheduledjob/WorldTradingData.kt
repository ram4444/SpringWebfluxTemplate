package main.kotlin.scheduledjob

import main.kotlin.controller.FuelController
import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import com.github.kittinunf.fuel.httpGet
import org.springframework.context.annotation.Configuration
import org.springframework.stereotype.Controller

@Configuration
@ConfigurationProperties(prefix = "wtd")
@Controller
class WorldTradingData {

    @Value("\${WorldTradingData.api_token}")                         val api_token: String = ""
    @Value("\${WorldTradingData.url_stock_realtime}")                val url_stock_realtime: String = ""
    @Value("\${WorldTradingData.url_stock_intraday}")                val url_stock_intraday: String = ""
    @Value("\${WorldTradingData.url_stock_history}")                 val url_stock_history: String = ""
    @Value("\${WorldTradingData.url_stock_multisingledayhistory}")   val url_stock_multiSingleDayHistory: String = ""
    @Value("\${WorldTradingData.url_fx_realtime}")                   val url_fx_realtime: String = ""
    @Value("\${WorldTradingData.url_fx_history}")                    val url_fx_history: String = ""
    @Value("\${WorldTradingData.url_fx_singledayhistory}")           val url_fx_singleDayHistory: String = ""

    /*
    lateinit var api_token: String
    lateinit var url: String

    @PostConstruct
    fun init() {
        api_token = ${wtd.api_token}"
        url:
    }
    */

    //private val map_api_token:Map<String,Any> = mapOf(Pair("api_token",api_token))
    private val logger = KotlinLogging.logger {}

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
    @Scheduled(fixedRate = 50000)
    fun getRealTimeStock(){
        /*
        https://www.worldtradingdata.com/api/v1/stock?symbol=AAPL,MSFT,HSBA.L&api_token=demo
        {
            "symbols_requested": 1,
            "symbols_returned": 1,
            "data": [
                {
                    "symbol": "AAPL",
                    "name": "Apple Inc.",
                    "price": "174.97",
                    "currency": "USD",
                    "price_open": "174.28",
                    "day_high": "175.15",
                    "day_low": "172.89",
                    "52_week_high": "233.47",
                    "52_week_low": "142.00",
                    "day_change": "1.82",
                    "change_pct": "1.05",
                    "close_yesterday": "173.15",
                    "market_cap": "825032547355",
                    "volume": "25886167",
                    "volume_avg": "28294177",
                    "shares": "4715280000",
                    "stock_exchange_long": "NASDAQ Stock Exchange",
                    "stock_exchange_short": "NASDAQ",
                    "timezone": "EST",
                    "timezone_name": "America/New_York",
                    "gmt_offset": "-18000",
                    "last_trade_time": "2019-03-01 16:00:01"
                }
            ]
        }
         */
        val json_ReqBody:String
        val json_map_rtnStr:String

        logger.info("The time is now ${DateTimeFormatter.ISO_LOCAL_TIME.format(LocalDateTime.now())}")

        val param:List<Pair<String, Any?>> = listOf(Pair("api_token", api_token), Pair("symbol", "AAPL,MSFT,HSBA.L"))
        val header_map:Map<String,Any>? = mapOf(
                "api_token" to api_token,
                "symbol" to "AAPL,MSFT,HSBA.L"
        )
        val (request, response, result) = url_stock_realtime.httpGet(param).responseString()
        logger.debug{"here"}
        logger.debug{"Request: ${request}" }
        logger.debug{"Response: ${response}" }
        logger.debug{"Result: ${result}" }
        //val fuelRtnMap = FuelController().curlByfuel("get", url_stock_realtime,  json_ReqBody)
    }

    fun getRealTimeFx(){
        /*
        https://www.worldtradingdata.com/api/v1/forex?base=USD&sort=newest&api_token=demo
        {
            "symbols_returned": 147
            "base": "USD",
            "data": {
                AED: "3.673000",
                AFN: "74.600000",
                ALL: "110.300000",
                AMD: "490.180000",
                ANG: "1.814900",
                AOA: "314.172000",
                ARS: "39.825000",
                AUD: "1.411800",
                AWG: "1.801250",
                AZN: "1.705000",
                ...
            }
        }
         */
    }

    fun getIntraDay(){
        /*
        https://www.worldtradingdata.com/api/v1/intraday?symbol=AAPL&range=1&interval=1&api_token=demo
        {
            "symbol": "AAPL",
            "stock_exchange_short": "NASDAQ",
            "timezone_name": "America/New_York",
            "intraday": {
                2018-10-19 15:59:00: {
                    "open": "219.49",
                    "close": "219.23",
                    "high": "219.61",
                    "low": "219.19",
                    "volume": "302415",
                },
                2018-10-19 15:58:00: {
                    "open": "219.62",
                    "close": "219.48",
                    "high": "219.70",
                    "low": "219.48",
                    "volume": "173762",
                },
                ...
            }
        }
         */
        logger.info("The time is now ${DateTimeFormatter.ISO_LOCAL_TIME.format(LocalDateTime.now())}")

    }

    fun getHistoryStock(){
        /*
        https://www.worldtradingdata.com/api/v1/history?symbol=AAPL&sort=newest&api_token=demo
        {
            "name": "AAPL",
            "history": {
                2019-02-28: {
                    "open": "174.32",
                    "close": "173.15",
                    "high": "174.91",
                    "low": "172.92",
                    "volume": "28215416"
                },
                2019-02-26: {
                    "open": "173.71",
                    "close": "174.33",
                    "high": "175.30",
                    "low": "173.17",
                    "volume": "17070211"
                ...
            }
        }
         */
    }

    fun getMultiSingleDayHistoryStock(){
        /*
        https://www.worldtradingdata.com/api/v1/history_multi_single_day?symbol=AAPL,MSFT&date=2018-01-02&api_token=demo
        {
            "date": "2018-01-02",
            "data": {
                AAPL: {
                    "open": "170.16",
                    "close": "172.26",
                    "high": "172.30",
                    "low": "169.26",
                    "volume": "25555934"
                },
                MSFT: {
                    "open": "86.13",
                    "close": "85.95",
                    "high": "86.31",
                    "low": "85.50",
                    "volume": "22483797"
                }
            }
        }
        */
    }

    fun getHistoryFx(){
        /*
        https://www.worldtradingdata.com/api/v1/forex_history?base=USD&convert_to=GBP&sort=newest&api_token=demo
        {
            "symbol": "USDGBP",
            "history": {
                2019-03-01: "0.757000",
                2019-02-28: "0.753900",
                2019-02-27: "0.750885",
                2019-02-26: "0.754255",
                2019-02-25: "0.761820",
                2019-02-24: "0.765410",
                2019-02-23: "0.766080",
                2019-02-22: "0.766110",
                2019-02-21: "0.766835",
                2019-02-20: "0.766595",
                ...
            }
        }
         */
    }

    fun getSingleDayFx(){
        /*
        https://www.worldtradingdata.com/api/v1/forex_single_day?base=USD&date=2018-08-31&api_token=demo
        {
            "total_returned": 5,
            "total_results": 5,
            "total_pages": 1,
            "limit": 50,
            "page": 1,
            "data": [
                {
                    "symbol": "AAPL",
                    "name": "Apple Inc.",
                    "currency": "USD",
                    "price": "174.97",
                    "stock_exchange_long": "NASDAQ Stock Exchange",
                    "stock_exchange_short": "NASDAQ",
                },
                ...
            ]
        }
         */
    }
}