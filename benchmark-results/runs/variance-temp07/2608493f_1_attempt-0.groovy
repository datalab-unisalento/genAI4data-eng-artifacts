import groovy.json.JsonSlurper
import groovy.json.JsonOutput

def sourceUrl = "http://recorder:8090/source/open-meteo/particulate?run_id=2608493f_1&attempt=0"
def sinkUrl = "http://recorder:8090/sink/2608493f_1?attempt=0"

def connection = new URL(sourceUrl).openConnection()
def jsonSlurper = new JsonSlurper()

def jsonData = jsonSlurper.parse(connection.inputStream)
def times = jsonData.hourly.time
def pm25Values = jsonData.hourly.pm2_5

def results = []

for (int i = 2; i < times.size(); i++) {
    def currentPm25 = pm25Values[i] as double
    def prev1Pm25 = pm25Values[i - 1] as double
    def prev2Pm25 = pm25Values[i - 2] as double

    def peak3h = [currentPm25, prev1Pm25, prev2Pm25].max()

    results << [
        time: times[i],
        pm2_5: currentPm25,
        pm2_5_peak_3h: peak3h
    ]
}

def jsonOutput = JsonOutput.toJson(results)

def sinkConnection = new URL(sinkUrl).openConnection()
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
