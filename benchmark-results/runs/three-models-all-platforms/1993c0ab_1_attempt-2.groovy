import groovy.json.JsonSlurper
import groovy.json.JsonOutput

def sourceUrl = "http://recorder:8090/source/open-meteo/particulate?run_id=1993c0ab_1&attempt=2"
def sinkUrl = "http://recorder:8090/sink/1993c0ab_1?attempt=2"

def sourceConnection = new URL(sourceUrl).openConnection()
sourceConnection.connectTimeout = 5000
sourceConnection.readTimeout = 5000

def sourceText = sourceConnection.inputStream.text
def jsonSlurper = new JsonSlurper()
def data = jsonSlurper.parseText(sourceText)

def summaries = [:]

data.hourly.time.eachWithIndex { time, index ->
    def date = time[0..9]
    if (!summaries[date]) {
        summaries[date] = [
            hours: 1,
            pm2_5_max: data.hourly.pm2_5[index],
            pm10_max: data.hourly.pm10[index]
        ]
    } else {
        summaries[date].hours++
        summaries[date].pm2_5_max = Math.max(summaries[date].pm2_5_max, data.hourly.pm2_5[index])
        summaries[date].pm10_max = Math.max(summaries[date].pm10_max, data.hourly.pm10[index])
    }
}

def records = summaries.collect { date, summary ->
    [
        date: date,
        hours: summary.hours,
        pm2_5_max: summary.pm2_5_max,
        pm10_max: summary.pm10_max
    ]
}

def sinkConnection = new URL(sinkUrl).openConnection()
sinkConnection.setRequestMethod("POST")
sinkConnection.setDoOutput(true)
sinkConnection.setRequestProperty("Content-Type", "application/json")

def jsonBody = JsonOutput.toJson([records: records])
sinkConnection.outputStream.write(jsonBody.bytes)

def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
