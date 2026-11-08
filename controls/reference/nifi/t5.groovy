import groovy.json.JsonOutput
import groovy.json.JsonSlurper

def fetch = { url -> new JsonSlurper().parseText(new URL(url).text) }
def particulate = fetch("http://recorder:8090/source/open-meteo/particulate?run_id=ref-t5&attempt=0")
def s = [particulate: particulate]

def h = s.particulate.hourly
def v = h.pm2_5
def records = (2..<v.size()).collect { i ->
    [time: h.time[i], pm2_5: v[i], pm2_5_peak_3h: [v[i - 2], v[i - 1], v[i]].max()]
}

def connection = new URL("http://recorder:8090/sink/ref-t5?attempt=0").openConnection()
connection.requestMethod = "POST"
connection.doOutput = true
connection.setRequestProperty("Content-Type", "application/json")
connection.outputStream.withWriter("UTF-8") { it << JsonOutput.toJson(records) }
if (connection.responseCode >= 300) {
    throw new RuntimeException("sink returned HTTP " + connection.responseCode)
}

def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
