import groovy.json.JsonSlurper
import groovy.json.JsonOutput

def source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=931ff7f5_1&attempt=0"
def sink_url = "http://recorder:8090/sink/931ff7f5_1?attempt=0"

def connection = new URL(source_url).openConnection()
def jsonSlurper = new JsonSlurper()

def responseBody = connection inputStream: true
def parsedJson = jsonSlurper.parse(responseBody)

def records = []

parsedJson.features.each { feature ->
    def properties = feature.properties
    def geometry = feature.geometry
    
    def magnitude_band
    if (properties.mag < 4.0) {
        magnitude_band = "minor"
    } else if (properties.mag >= 5.0) {
        magnitude_band = "major"
    } else {
        magnitude_band = "moderate"
    }

    records << [
        id: feature.id,
        magnitude: properties.mag,
        place: properties.place,
        time_ms: properties.time,
        longitude: geometry.coordinates[0],
        latitude: geometry.coordinates[1],
        depth_km: geometry.coordinates[2],
        magnitude_band: magnitude_band
    ]
}

def jsonOutput = JsonOutput.toJson(records)

def sinkConnection = new URL(sink_url).openConnection()
sinkConnection.setRequestMethod("POST")
sinkConnection.setDoOutput(true)
sinkConnection.setRequestProperty("Content-Type", "application/json")

def outputStream = sinkConnection.getOutputStream()
outputStream.write(jsonOutput.getBytes())
outputStream.close()

def responseCode = sinkConnection.responseCode
if (responseCode >= 300) {
    throw new RuntimeException("Failed to send data: HTTP ${responseCode}")
}

def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
