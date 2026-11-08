import groovy.json.JsonSlurper
import groovy.json.JsonOutput

def source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=b620c0e5_1&attempt=1"
def sink_url = "http://recorder:8090/sink/b620c0e5_1?attempt=1"

def connection = new URL(source_url).openConnection()
def response = connection.getInputStream().getText()
def jsonSlurper = new JsonSlurper()
def data = jsonSlurper.parseText(response)

def records = []
def dailySummary = [:]

def times = data.hourly.time
def pm10Values = data.hourly.pm10
def pm25Values = data.hourly.pm2_5

for (int i = 0; i < times.size(); i++) {
    def date = times[i].take(10)
    def pm10 = pm10Values[i]
    def pm25 = pm25Values[i]

    if (!dailySummary.containsKey(date)) {
        dailySummary[date] = [
            date: date,
            hours: 0,
            pm2_5_max: null,
            pm10_max: null
        ]
    }

    def summary = dailySummary[date]
    summary.hours += 1

    if (summary.pm2_5_max == null || pm25 > summary.pm2_5_max) {
        summary.pm2_5_max = pm25
    }

    if (summary.pm10_max == null || pm10 > summary.pm10_max) {
        summary.pm10_max = pm10
    }
}

records.addAll(dailySummary.values())

def jsonOutput = JsonOutput.toJson(records)

def sinkConnection = new URL(sink_url).openConnection()
sinkConnection.setRequestMethod("POST")
sinkConnection.setDoOutput(true)
sinkConnection.setRequestProperty("Content-Type", "application/json")

def writer = new OutputStreamWriter(sinkConnection.getOutputStream())
writer.write(jsonOutput)
writer.flush()
writer.close()

def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
