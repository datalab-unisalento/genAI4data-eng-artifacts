import groovy.json.JsonSlurper
import groovy.json.JsonOutput

def sourceUrl = "http://recorder:8090/source/open-meteo/particulate?run_id=8c5cc664_1&attempt=1"
def sinkUrl = "http://recorder:8090/sink/8c5cc664_1?attempt=1"

def response = new URL(sourceUrl).openConnection().with { it.inputStream.text }
def data = new JsonSlurper().parse(response)

def hourly = data.hourly
def times = hourly.time as List
def pm25Values = hourly.'pm2_5' as List

def records = []
for (int i = 2; i < pm25Values.size(); i++) {
    def currentPm25 = pm25Values[i]
    def prev1Pm25 = pm25Values[i - 1]
    def prev2Pm25 = pm25Values[i - 2]
    
    def peak3h = [currentPm25, prev1Pm25, prev2Pm25].max()
    
    records.add([
        time: times[i],
        pm2_5: currentPm25,
        pm2_5_peak_3h: peak3h
    ])
}

def body = JsonOutput.toJson(records)

new URL(sinkUrl).openConnection().with {
    setRequestMethod("POST")
    setDoOutput(true)
    setRequestProperty("Content-Type", "application/json")
    getOutputStream().write(body.getBytes("UTF-8"))
    getInputStream().text
}

def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
