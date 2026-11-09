import groovy.json.JsonOutput
import groovy.json.JsonSlurper

def usgs = new JsonSlurper().parseText(new URL("http://recorder:8090/source/usgs/earthquakes?run_id=probe-get_dot&attempt=0").text)
def rows = usgs.features.collect { f -> [id: f.id, magnitude: f.get('properties').mag] }

def connection = new URL("http://recorder:8090/sink/probe-get_dot?attempt=0").openConnection()
connection.requestMethod = "POST"
connection.doOutput = true
connection.setRequestProperty("Content-Type", "application/json")
connection.outputStream.withWriter("UTF-8") { it << JsonOutput.toJson(rows) }
if (connection.responseCode >= 300) {
    throw new RuntimeException("sink returned HTTP " + connection.responseCode)
}

def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
