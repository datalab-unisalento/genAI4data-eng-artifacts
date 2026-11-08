import groovy.json.JsonOutput
import groovy.json.JsonSlurper

def sourceUrl = "http://recorder:8090/source/open-meteo/particulate?run_id=control-t1&attempt=0"
def sinkUrl = "http://recorder:8090/sink/control-t1?attempt=0"

def payload = new JsonSlurper().parseText(new URL(sourceUrl).text)
def hourly = payload.hourly
def records = []
hourly.time.eachWithIndex { time, index ->
    records << [time: time, pm10: hourly.pm10[index], pm2_5: hourly.pm2_5[index]]
}

def connection = new URL(sinkUrl).openConnection()
connection.requestMethod = "POST"
connection.doOutput = true
connection.setRequestProperty("Content-Type", "application/json")
connection.outputStream.withWriter("UTF-8") { it << JsonOutput.toJson(records) }
if (connection.responseCode >= 300) {
    throw new RuntimeException("sink returned HTTP " + connection.responseCode)
}

def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
