import groovy.json.JsonSlurper
import groovy.json.JsonOutput

def process() {
    def sourceUrl = "http://recorder:8090/source/open-meteo/particulate?run_id=40ac4850_1&attempt=0"
    def sinkUrl = "http://recorder:8090/sink/40ac4850_1?attempt=0"

    // Fetch and parse source data
    def connection = new URL(sourceUrl).openConnection()
    def jsonSlurper = new JsonSlurper()
    def data = jsonSlurper.parse(connection.inputStream)

    // Group records by date and calculate summaries
    def summaries = [:]
    for (int i = 0; i < data.hourly.time.size(); i++) {
        def timeStr = data.hourly.time[i]
        def dateKey = timeStr.substring(0, 10)
        
        if (!summaries.containsKey(dateKey)) {
            summaries[dateKey] = [
                hours: 0,
                pm2_5_max: Float.NEGATIVE_INFINITY,
                pm10_max: Float.NEGATIVE_INFINITY
            ]
        }
        
        def summary = summaries[dateKey]
        summary.hours++
        summary.pm2_5_max = Math.max(summary.pm2_5_max, data.hourly.pm2_5[i])
        summary.pm10_max = Math.max(summary.pm10_max, data.hourly.pm10[i])
    }

    // Convert to output format
    def records = summaries.collect { dateKey, summary ->
        [
            date: dateKey,
            hours: summary.hours,
            pm2_5_max: summary.pm2_5_max,
            pm10_max: summary.pm10_max
        ]
    }

    // Send to sink
    def jsonOutput = JsonOutput.toJson(records)
    connection = new URL(sinkUrl).openConnection()
    connection.setRequestMethod("POST")
    connection.setDoOutput(true)
    connection.setRequestProperty("Content-Type", "application/json")

    connection.outputStream.write(jsonOutput.bytes)

    if (connection.responseCode >= 300) {
        throw new Exception("Sink request failed with status: ${connection.responseCode}")
    }

    def flowFile = session.create()
    session.transfer(flowFile, REL_SUCCESS)
}

process()
