import groovy.json.JsonSlurper
import groovy.json.JsonOutput

def slurper = new JsonSlurper()
def sourceUrl = "http://recorder:8090/source/open-meteo/particulate?run_id=79957702_1&attempt=0"
def sinkUrl = "http://recorder:8090/sink/79957702_1?attempt=0"

def sourceConnection = new URL(sourceUrl).openConnection()
sourceConnection.connectTimeout = 5000
sourceConnection.readTimeout = 5000

def sourceData = slurper.parse(sourceConnection.inputStream)

def records = []
for (int i = 2; i < sourceData.hourly.time.size(); i++) {
    def time = sourceData.hourly.time[i]
    def pm2_5 = sourceData.hourly.pm2_5[i]
    
    // Calculate max of current and previous two hours
    def peak3h = Math.max(pm2_5, 
        Math.max(sourceData.hourly.pm2_5[i-1], sourceData.hourly.pm2_5[i-2]))
    
    records << [
        time: time,
        pm2_5: pm2_5,
        pm2_5_peak_3h: peak3h
    ]
}

def sinkConnection = new URL(sinkUrl).openConnection()
sinkConnection.setRequestMethod("POST")
sinkConnection.setDoOutput(true)
sinkConnection.setRequestProperty("Content-Type", "application/json")

def jsonBody = JsonOutput.toJson([records: records])
sinkConnection.outputStream.write(jsonBody.bytes)

def responseCode = sinkConnection.responseCode
if (responseCode >= 300) {
    throw new Exception("Sink request failed with status code ${responseCode}")
}

def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
