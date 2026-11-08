import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.net.HttpURLConnection
import java.net.URL

def sourceUrl = "http://recorder:8090/source/open-meteo/particulate?run_id=d429eb10_1&attempt=0"
def sinkUrl = "http://recorder:8090/sink/d429eb10_1?attempt=0"

def connection = new URL(sourceUrl).openConnection()
def jsonSlurper = new JsonSlurper()
def response = jsonSlurper.parse(connection.inputStream)

def hourly = response.hourly
def timeArray = hourly.time
def pm10Array = hourly.pm10
def pm25Array = hourly.pm2_5

def dailySummary = [:]

for (int i = 0; i < timeArray.size(); i++) {
    def date = timeArray[i].take(10)
    def pm10 = pm10Array[i]
    def pm25 = pm25Array[i]

    if (!dailySummary.containsKey(date)) {
        dailySummary[date] = [
            date: date,
            hours: 0,
            pm2_5_max: null,
            pm10_max: null
        ]
    }

    def summary = dailySummary[date]
    summary.hours += 1

    if (summary.pm10_max == null || pm10 > summary.pm10_max) {
        summary.pm10_max = pm10
    }

    if (summary.pm2_5_max == null || pm25 > summary.pm2_5_max) {
        summary.pm2_5_max = pm25
    }
}

def records = dailySummary.values().toList()

def jsonOutput = JsonOutput.toJson(records)

def sinkConnection = new URL(sinkUrl).openConnection()
sinkConnection.setRequestMethod("POST")
sinkConnection.setDoOutput(true)
sinkConnection.setRequestProperty("Content-Type", "application/json")

def outputStream = sinkConnection.getOutputStream()
outputStream.write(jsonOutput.getBytes("UTF-8"))
outputStream.flush()
outputStream.close()

def responseCode = sinkConnection.responseCode
if (responseCode >= 300) {
    throw new RuntimeException("HTTP error code: ${responseCode}")
}

def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
