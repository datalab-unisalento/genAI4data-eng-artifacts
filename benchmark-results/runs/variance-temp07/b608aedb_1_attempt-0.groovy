import groovy.json.JsonSlurper
import groovy.json.JsonOutput

def sourceUrl = "http://recorder:8090/source/open-meteo/particulate?run_id=b608aedb_1&attempt=0"
def sinkUrl = "http://recorder:8090/sink/b608aedb_1?attempt=0"

def connection = new URL(sourceUrl).openConnection()
def jsonSlurper = new JsonSlurper()
def response = jsonSlurper.parse(connection.inputStream)

def times = response.hourly.time
def pm25Values = response.hourly.pm2_5

def results = []

for (int i = 2; i < times.size(); i++) {
    def currentPm25 = pm25Values[i]
    def previousPm25 = pm25Values[i - 1]
    def twoHoursAgoPm25 = pm25Values[i - 2]

    def maxPm25 = [currentPm25, previousPm25, twoHoursAgoPm25].max()

    results << [
        time: times[i],
        pm2_5: currentPm25,
        pm2_5_peak_3h: maxPm25
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
