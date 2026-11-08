import groovy.json.JsonSlurper
import groovy.json.JsonOutput

def source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=ac9a4917_1&attempt=0"
def sink_url = "http://recorder:8090/sink/ac9a4917_1?attempt=0"

def connection = new URL(source_url).openConnection()
def jsonSlurper = new JsonSlurper()
def responseBody = connection.inputStream.text
def data = jsonSlurper.parseText(responseBody)

def pm25Array = data.hourly.pm2_5
def timeArray = data.hourly.time

def results = []

for (int i = 2; i < pm25Array.size(); i++) {
    def current_pm25 = pm25Array[i]
    def previous_pm25 = pm25Array[i - 1]
    def two_hours_ago_pm25 = pm25Array[i - 2]

    def peak = [current_pm25, previous_pm25, two_hours_ago_pm25].max()

    results << [
        time: timeArray[i],
        pm2_5: current_pm25,
        pm2_5_peak_3h: peak
    ]
}

def jsonOutput = JsonOutput.toJson(results)

def sinkConnection = new URL(sink_url).openConnection()
sinkConnection.setRequestMethod("POST")
sinkConnection.setDoOutput(true)
sinkConnection.setRequestProperty("Content-Type", "application/json")

def outputStream = sinkConnection.getOutputStream()
outputStream.write(jsonOutput.getBytes("UTF-8"))
outputStream.close()

def responseCode = sinkConnection.responseCode
if (responseCode >= 300) {
    throw new RuntimeException("HTTP error code: ${responseCode}")
}

def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
