import groovy.json.JsonSlurper
import groovy.json.JsonOutput

def sourceUrl = "http://recorder:8090/source/usgs/earthquakes?run_id=ee82c012_1&attempt=0"
def sinkUrl = "http://recorder:8090/sink/ee82c012_1?attempt=0"

def sourceConnection = new URL(sourceUrl).openConnection()
sourceConnection.connectTimeout = 5000
sourceConnection.readTimeout = 5000

def jsonSlurper = new JsonSlurper()
def geoJsonData = jsonSlurper.parse(sourceConnection.inputStream)

def records = []
geoJsonData.features.each { feature ->
    def record = [
        id: feature.id,
        magnitude: feature.properties.mag,
        place: feature.properties.place,
        time_ms: feature.properties.time,
        longitude: feature.geometry.coordinates[0],
        latitude: feature.geometry.coordinates[1],
        depth_km: feature.geometry.coordinates[2]
    ]
    
    if (record.magnitude < 4.0) {
        record.magnitude_band = "minor"
    } else if (record.magnitude >= 5.0) {
        record.magnitude_band = "major"
    } else {
        record.magnitude_band = "moderate"
    }
    
    records << record
}

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

def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
