import groovy.json.JsonSlurper
import groovy.json.JsonOutput

def source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=bb02eb55_1&attempt=0"
def sink_url = "http://recorder:8090/sink/bb02eb55_1?attempt=0"

def text = new URL(source_url).openConnection().getInputStream.text
def collection = new JsonSlurper().parse(text)
def features = collection.features

def records = []
features.each { feature ->
    def props = feature.properties
    def geom = feature.geometry
    def coords = geom.coordinates
    double magnitude = props.mag as double
    String magnitude_band
    if (magnitude < 4.0) {
        magnitude_band = "minor"
    } else if (magnitude < 5.0) {
        magnitude_band = "moderate"
    } else {
        magnitude_band = "major"
    }
    def record = [
            id: feature.id,
            magnitude: magnitude,
            place: props.place,
            time_ms: props.time,
            longitude: coords[0],
            latitude: coords[1],
            depth_km: coords[2],
            magnitude_band: magnitude_band
    ]
    records.add(record)
}

def body = JsonOutput.toJson([records: records])
def conn = new URL(sink_url).openConnection()
conn.setRequestMethod("POST")
conn.setRequestProperty("Content-Type", "application/json")
conn.setDoOutput(true)
conn.getOutputStream().write(body.bytes)
conn.getInputStream.text
session.create()
session.transfer(flowFile, REL_SUCCESS)
