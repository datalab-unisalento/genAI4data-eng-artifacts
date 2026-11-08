import groovy.json.JsonSlurper
import groovy.json.JsonOutput

def source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=027624a4_1&attempt=2"
def sink_url = "http://recorder:8090/sink/027624a4_1?attempt=2"

def connection = new URL(source_url).openConnection()
def jsonText = connection.inputStream.text
def data = new JsonSlurper().parseText(jsonText)

def records = []

data.features.each { feature ->
    def id = feature.id
    def magnitude = feature.properties.mag
    def place = feature.properties.place
    def time_ms = feature.properties.time
    def longitude = feature.geometry.coordinates[0]
    def latitude = feature.geometry.coordinates[1]
    def depth_km = feature.geometry.coordinates[2]

    def magnitude_band
    if (magnitude < 4.0) {
        magnitude_band = "minor"
    } else if (magnitude >= 5.0) {
        magnitude_band = "major"
    } else {
        magnitude_band = "moderate"
    }

    records << [
        id: id,
        magnitude: magnitude,
        place: place,
        time_ms: time_ms,
        longitude: longitude,
        latitude: latitude,
        depth_km: depth_km,
        magnitude_band: magnitude_band
    ]
}

def jsonOutput = JsonOutput.toJson(records)

def sinkConnection = new URL(sink_url).openConnection()
sinkConnection.setRequestMethod("POST")
sinkConnection.setDoOutput(true)
sinkConnection.setRequestProperty("Content-Type", "application/json")

sinkConnection.outputStream.write(jsonOutput.bytes)
sinkConnection.outputStream.close()

def responseCode = sinkConnection.responseCode
if (responseCode >= 300) {
    throw new RuntimeException("HTTP error code: $responseCode")
}

def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
