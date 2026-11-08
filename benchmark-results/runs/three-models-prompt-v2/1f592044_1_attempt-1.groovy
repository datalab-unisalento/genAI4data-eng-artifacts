import groovy.json.JsonOutput

def sourceUrl = 'http://recorder:8090/source/open-meteo/particulate?run_id=1f592044_1&attempt=1'
def sinkUrl = 'http://recorder:8090/sink/1f592044_1?attempt=1'

def sourceConnection = new URL(sourceUrl).openConnection()
sourceConnection.connectTimeout = 5000
sourceConnection.readTimeout = 5000

def sourceJsonText = sourceConnection.inputStream.text
def sourceData = new groovy.json.JsonSlurper().parseText(sourceJsonText)

def records = []
for (int i = 0; i < sourceData.hourly.time.size(); i++) {
    def record = [
        time: sourceData.hourly.time[i],
        pm10: sourceData.hourly.pm10[i],
        pm2_5: sourceData.hourly.pm2_5[i]
    ]
    records << record
}

def sinkBody = JsonOutput.toJson([records: records])

def sinkConnection = new URL(sinkUrl).openConnection()
sinkConnection.setRequestMethod('POST')
sinkConnection.setDoOutput(true)
sinkConnection.setRequestProperty('Content-Type', 'application/json')

sinkConnection.outputStream.write(sinkBody.bytes)

def responseCode = sinkConnection.responseCode
if (responseCode >= 300) {
    throw new Exception("Sink request failed with status code: ${responseCode}")
}

def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
