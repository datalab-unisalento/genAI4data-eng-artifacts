def sourceUrl = "http://recorder:8090/source/open-meteo/particulate?run_id=63dfa614_1&attempt=2"
def sinkUrl = "http://recorder:8090/sink/63dfa614_1?attempt=2"

def raw = new URL(sourceUrl).openConnection().getInputStream().text
def data = new groovy.json.JsonSlurper().parseText(raw)

def hourly = data.hourly
def times = hourly.time as List<String>
def pm25 = hourly.pm2_5 as List<Number>
def pm10 = hourly.pm10 as List<Number>

def dateGroups = [:]
times.eachWithIndex { t, i ->
    def date = t.substring(0, 10)
    if (!dateGroups.containsKey(date)) {
        dateGroups[date] = []
    }
    dateGroups[date].add([pm25: pm25[i], pm10: pm10[i]])
}

def summaries = dateGroups.entries.collect { date, records ->
    def pm25max = records.max { it.pm25 } as Number
    def pm10max = records.max { it.pm10 } as Number
    [date: date, hours: records.size(), pm2_5_max: pm25max, pm10_max: pm10max]
}

def body = groovy.json.JsonOutput.toJson(summaries)

def connection = new URL(sinkUrl).openConnection()
connection.setRequestMethod("POST")
connection.setRequestProperty("Content-Type", "application/json")
connection.setDoOutput(true)
connection.outputStream.write(body.getBytes())
connection.responseCode
if (connection.responseCode >= 300) {
    throw new RuntimeException("HTTP status: ${connection.responseCode}")
}
def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
