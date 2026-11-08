import groovy.json.JsonOutput
import groovy.json.JsonSlurper

def fetch = { url -> new JsonSlurper().parseText(new URL(url).text) }
def usgs = fetch("http://recorder:8090/source/usgs/earthquakes?run_id=ref-t2&attempt=0")
def s = [usgs: usgs]

def records = s.usgs.features.collect { f ->
    def mag = f.get('properties').get('mag')
    def c = f.geometry.coordinates
    [id: f.id, magnitude: mag, place: f.get('properties').get('place'), time_ms: f.get('properties').get('time'),
     longitude: c[0], latitude: c[1], depth_km: c[2],
     magnitude_band: mag < 4.0 ? "minor" : (mag < 5.0 ? "moderate" : "major")]
}

def connection = new URL("http://recorder:8090/sink/ref-t2?attempt=0").openConnection()
connection.requestMethod = "POST"
connection.doOutput = true
connection.setRequestProperty("Content-Type", "application/json")
connection.outputStream.withWriter("UTF-8") { it << JsonOutput.toJson(records) }
if (connection.responseCode >= 300) {
    throw new RuntimeException("sink returned HTTP " + connection.responseCode)
}

def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
