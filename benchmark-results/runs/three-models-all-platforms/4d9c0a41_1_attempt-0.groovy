import groovy.json.JsonSlurper
import groovy.json.JsonOutput

def source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=4d9c0a41_1&attempt=0"
def sink_url = "http://recorder:8090/sink/4d9c0a41_1?attempt=0"

def connection = new URL(source_url).openConnection()
def response = connection.getInputStream().getText()
def jsonSlurper = new JsonSlurper()
def data = jsonSlurper.parseText(response)

def records = []
def times = data.hourly.time
def pm10s = data.hourly.pm10
def pm2_5s = data.hourly.pm2_5

for (int i = 0; i < times.size(); i++) {
    def record = [
        time: times[i],
        pm10: pm10s[i],
        pm2_5: pm2_5s[i]
    ]
    records.add(record)
}

def jsonOutput = new JsonOutput()
def payload = jsonOutput.toJson(records)

def sinkConnection = new URL(sink_url).openConnection()
sinkConnection.setRequestMethod("POST")
sinkConnection.setDoOutput(true)
sinkConnection.setRequestProperty("Content-Type", "application/json")

def writer = new OutputStreamWriter(sinkConnection.getOutputStream())
writer.write(payload)
writer.flush()
writer.close()

sinkConnection.getInputStream().close()

def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
