import groovy.json.JsonSlurper
import groovy.json.JsonOutput

def sourceUrl = "http://recorder:8090/source/open-meteo/particulate?run_id=d4488e80_1&attempt=0"
def sinkUrl = "http://recorder:8090/sink/d4488e80_1?attempt=0"

def connection = new URL(sourceUrl).openConnection()
def jsonResponse = connection.getInputStream().getText()
def jsonSlurper = new JsonSlurper()
def data = jsonSlurper.parseText(jsonResponse)

def times = data.hourly.time
def pm25Values = data.hourly.pm2_5

def results = []

for (int i = 2; i < times.size(); i++) {
    def currentPm25 = pm25Values[i]
    def previousPm25 = pm25Values[i - 1]
    def twoHoursAgoPm25 = pm25Values[i - 2]

    def peak3h = [currentPm25, previousPm25, twoHoursAgoPm25].max()

    results.add([
        time: times[i],
        pm2_5: currentPm25,
        pm2_5_peak_3h: peak3h
    ])
}

def jsonOutput = JsonOutput.toJson(results)

def sinkConnection = new URL(sinkUrl).openConnection()
sinkConnection.setRequestMethod("POST")
sinkConnection.setDoOutput(true)
sinkConnection.setRequestProperty("Content-Type", "application/json")

def writer = new OutputStreamWriter(sinkConnection.getOutputStream())
writer.write(jsonOutput)
writer.flush()
writer.close()

sinkConnection.getInputStream().close()

def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
