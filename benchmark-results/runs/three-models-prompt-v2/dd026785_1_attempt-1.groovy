import groovy.json.JsonSlurper
import groovy.json.JsonOutput

def source_url = "http://recorder:8090/source/open-meteo/particulate?run_id=dd026785_1&attempt=1"
def sink_url = "http://recorder:8090/sink/dd026785_1?attempt=1"

def connection = new URL(source_url).openConnection()
def jsonSlurper = new JsonSlurper()

def response = connection.getInputStream().getText('UTF-8')
def data = jsonSlurper.parseText(response)

def times = data.hourly.time
def pm2_5_values = data.hourly.pm2_5

def results = []

for (int i = 2; i < times.size(); i++) {
    def current_pm2_5 = pm2_5_values[i]
    def previous_pm2_5 = pm2_5_values[i - 1]
    def two_hours_ago_pm2_5 = pm2_5_values[i - 2]

    def peak_3h = [current_pm2_5, previous_pm2_5, two_hours_ago_pm2_5].max()

    results << [
        time: times[i],
        pm2_5: current_pm2_5,
        pm2_5_peak_3h: peak_3h
    ]
}

def sinkConnection = new URL(sink_url).openConnection()
sinkConnection.setRequestMethod("POST")
sinkConnection.setDoOutput(true)
sinkConnection.setRequestProperty("Content-Type", "application/json")

def jsonBody = JsonOutput.toJson(results)
sinkConnection.getOutputStream().write(jsonBody.getBytes('UTF-8'))

def responseCode = sinkConnection.responseCode
if (responseCode >= 300) {
    throw new RuntimeException("HTTP error code: ${responseCode}")
}

def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
