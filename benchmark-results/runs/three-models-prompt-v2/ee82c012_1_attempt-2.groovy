import groovy.json.JsonSlurper
import groovy.json.JsonOutput

def sourceUrl = "http://recorder:8090/source/usgs/earthquakes?run_id=ee82c012_1&attempt=2"
def sinkUrl = "http://recorder:8090/sink/ee82c012_1?attempt=2"

// Read and parse source data
def sourceConnection = new URL(sourceUrl).openConnection()
sourceConnection.connectTimeout = 5000
sourceConnection.readTimeout = 5000

def jsonSlurper = new JsonSlurper()
def earthquakeData = jsonSlurper.parse(sourceConnection.inputStream)

// Process each feature into a record
def records = []
earthquakeData.features.each { feature ->
    def geometry = feature.geometry.coordinates
    def magnitudeBand
    
    if (feature.properties.mag < 4.0) {
        magnitudeBand = "minor"
    } else if (feature.properties.mag >= 5.0) {
        magnitudeBand = "major"
    } else {
        magnitudeBand = "moderate"
    }
    
    records << [
        id: feature.id,
        magnitude: feature.properties.mag,
        place: feature.properties.place,
        time_ms: feature.properties.time,
        longitude: geometry[0],
        latitude: geometry[1],
        depth_km: geometry[2],
        magnitude_band: magnitudeBand
    ]
}

// Send records to sink
def sinkConnection = new URL(sinkUrl).openConnection()
sinkConnection.setRequestMethod("POST")
sinkConnection.setDoOutput(true)
sinkConnection.setRequestProperty("Content-Type", "application/json")

def jsonBody = JsonOutput.toJson([records: records])
sinkConnection.outputStream.write(jsonBody.bytes)

def responseCode = sinkConnection.responseCode
if (responseCode >= 300) {
    throw new Exception("Sink request failed with status code ${responseCode}")
}

// Create and transfer flow file
def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
