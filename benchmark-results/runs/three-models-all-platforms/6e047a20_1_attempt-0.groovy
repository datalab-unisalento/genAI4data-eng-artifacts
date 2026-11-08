import groovy.json.JsonSlurper
import groovy.json.JsonOutput

def sourceUrl = "http://recorder:8090/source/open-meteo/particulate?run_id=6e047a20_1&attempt=0"
def sinkUrl = "http://recorder:8090/sink/6e047a20_1?attempt=0"

def slurper = new JsonSlurper()
def sourceResponse = new URL(sourceUrl).openConnection().inputStream.text
def data = slurper.parseText(sourceResponse)

def times = data.hourly.time
def pm25Values = data.hourly.pm2_5

def results = []
for (int i = 2; i < times.size(); i++) {
    def currentPm25 = pm25Values[i]
    def previousTwoPm25s = [pm25Values[i-1], pm25Values[i-2]]
    
    def maxPeak3h = Math.max(currentPm25, Math.max(previousTwoPm25s[0], previousTwoPm25s[1]))
    
    results << [
        time: times[i],
        pm2_5: currentPm25,
        pm2_5_peak_3h: maxPeak3h
    ]
}

def sinkBody = JsonOutput.toJson(results)
def connection = new URL(sinkUrl).openConnection()
connection.setRequestMethod("POST")
connection.setDoOutput(true)
connection.setRequestProperty("Content-Type", "application/json")

def writer = new OutputStreamWriter(connection.outputStream)
writer.write(sinkBody)
writer.flush()

def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
