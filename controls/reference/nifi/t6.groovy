import groovy.json.JsonOutput
import groovy.json.JsonSlurper

def fetch = { url -> new JsonSlurper().parseText(new URL(url).text) }
def usgs = fetch("http://recorder:8090/source/usgs/earthquakes?run_id=ref-t6&attempt=0")
def s = [usgs: usgs]

def bands = [:]
s.usgs.features.each { f ->
    def mag = f.get('properties').get('mag')
    def depth = f.geometry.coordinates[2]
    def name = mag < 4.0 ? "minor" : (mag < 5.0 ? "moderate" : "major")
    if (!bands.containsKey(name)) bands[name] = [event_count: 0, max_magnitude: null, max_depth_km: null]
    def b = bands[name]
    b.event_count += 1
    b.max_magnitude = b.max_magnitude == null ? mag : Math.max(b.max_magnitude, mag)
    b.max_depth_km = b.max_depth_km == null ? depth : Math.max(b.max_depth_km, depth)
}
def records = ["minor", "moderate", "major"].findAll { bands.containsKey(it) }.collect { [magnitude_band: it] + bands[it] }

def connection = new URL("http://recorder:8090/sink/ref-t6?attempt=0").openConnection()
connection.requestMethod = "POST"
connection.doOutput = true
connection.setRequestProperty("Content-Type", "application/json")
connection.outputStream.withWriter("UTF-8") { it << JsonOutput.toJson(records) }
if (connection.responseCode >= 300) {
    throw new RuntimeException("sink returned HTTP " + connection.responseCode)
}

def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
