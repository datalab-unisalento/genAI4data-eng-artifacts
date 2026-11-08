import groovy.json.JsonSlurper
import groovy.json.JsonOutput

def sourceUrl = 'http://recorder:8090/source/open-meteo/particulate?run_id=33e11636_1&attempt=0'
def sinkUrl = 'http://recorder:8090/sink/33e11636_1?attempt=0'

def sourceConnection = new URL(sourceUrl).openConnection()
sourceConnection.connectTimeout = 5000
sourceConnection.readTimeout = 5000

def sourceResponse = sourceConnection.inputStream.text
def jsonSlurper = new JsonSlurper()
def data = jsonSlurper.parseText(sourceResponse)

def records = []
for (int i = 0; i < data.hourly.time.size(); i++) {
    def record = [
        time: data.hourly.time[i],
        pm10: data.hourly.pm10[i],
        pm2_5: data.hourly.pm2_5[i]
    ]
    records << record
}

def sinkConnection = new URL(sinkUrl).openConnection()
sinkConnection.setRequestMethod('POST')
sinkConnection.setDoOutput(true)
sinkConnection.setRequestProperty('Content-Type', 'application/json')

def jsonBody = JsonOutput.toJson([records: records])
sinkConnection.outputStream.write(jsonBody.bytes)

def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
