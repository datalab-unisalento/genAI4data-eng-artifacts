import groovy.json.JsonOutput
import groovy.json.JsonSlurper

def fetch = { url -> new JsonSlurper().parseText(new URL(url).text) }
def particulate = fetch("http://recorder:8090/source/open-meteo/particulate?run_id=ref-t3&attempt=0")
def gases = fetch("http://recorder:8090/source/open-meteo/gases?run_id=ref-t3&attempt=0")
def s = [particulate: particulate, gases: gases]

def p = s.particulate.hourly
def g = s.gases.hourly
def gasIndex = [:]
(0..<g.time.size()).each { gasIndex[g.time[it]] = [no2: g.nitrogen_dioxide[it], o3: g.ozone[it]] }
def records = []
(0..<p.time.size()).each { i ->
    def t = p.time[i]
    if (gasIndex.containsKey(t)) {
        records << [time: t, pm10: p.pm10[i], pm2_5: p.pm2_5[i],
                    nitrogen_dioxide: gasIndex[t].no2, ozone: gasIndex[t].o3]
    }
}

def connection = new URL("http://recorder:8090/sink/ref-t3?attempt=0").openConnection()
connection.requestMethod = "POST"
connection.doOutput = true
connection.setRequestProperty("Content-Type", "application/json")
connection.outputStream.withWriter("UTF-8") { it << JsonOutput.toJson(records) }
if (connection.responseCode >= 300) {
    throw new RuntimeException("sink returned HTTP " + connection.responseCode)
}

def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
