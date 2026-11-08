import groovy.json.JsonOutput
import groovy.json.JsonSlurper

def fetch = { url -> new JsonSlurper().parseText(new URL(url).text) }
def particulate = fetch("http://recorder:8090/source/open-meteo/particulate?run_id=ref-t4&attempt=0")
def s = [particulate: particulate]

def h = s.particulate.hourly
def days = [:]
(0..<h.time.size()).each { i ->
    def d = h.time[i].substring(0, 10)
    if (!days.containsKey(d)) days[d] = [hours: 0, pm2_5_max: null, pm10_max: null]
    days[d].hours += 1
    def p25 = h.pm2_5[i]
    def p10 = h.pm10[i]
    days[d].pm2_5_max = days[d].pm2_5_max == null ? p25 : Math.max(days[d].pm2_5_max, p25)
    days[d].pm10_max = days[d].pm10_max == null ? p10 : Math.max(days[d].pm10_max, p10)
}
def records = days.keySet().sort().collect { [date: it] + days[it] }

def connection = new URL("http://recorder:8090/sink/ref-t4?attempt=0").openConnection()
connection.requestMethod = "POST"
connection.doOutput = true
connection.setRequestProperty("Content-Type", "application/json")
connection.outputStream.withWriter("UTF-8") { it << JsonOutput.toJson(records) }
if (connection.responseCode >= 300) {
    throw new RuntimeException("sink returned HTTP " + connection.responseCode)
}

def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
