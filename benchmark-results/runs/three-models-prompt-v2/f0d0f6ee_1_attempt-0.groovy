import groovy.json.JsonSlurper
import groovy.json.JsonOutput

def source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=f0d0f6ee_1&attempt=0"
def sink_url = "http://recorder:8090/sink/f0d0f6ee_1?attempt=0"

def connection = new URL(source_url).openConnection()
def jsonSlurper = new JsonSlurper()
def response = jsonSlurper.parse(connection.inputStream)

def records = []
def times = response.hourly.time
def pm10s = response.hourly.pm10
def pm2_5s = response.hourly.pm2_5

for (int i = 0; i < times.size(); i++) {
    def record = [
        time: times[i],
        pm10: pm10s[i],
        pm2_5: pm2_5s[i]
    ]
    records.add(record)
}

def sinkConnection = new URL(sink_url).openConnection()
sinkConnection.setRequestMethod("POST")
sinkConnection.setDoOutput(true)
sinkConnection.setRequestProperty("Content-Type", "application/json")

def jsonBody = JsonOutput.toJson(records)
sinkConnection.outputStream.write(jsonBody.getBytes("UTF-8"))
sinkConnection.outputStream.close()

def responseCode = sinkConnection.responseCode
if (responseCode >= 300) {
    throw new RuntimeException("HTTP error code: ${responseCode}")
}

def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
