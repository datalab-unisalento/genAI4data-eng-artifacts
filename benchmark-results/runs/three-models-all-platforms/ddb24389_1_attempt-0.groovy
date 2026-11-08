import groovy.json.JsonSlurper
import groovy.json.JsonOutput

def sourceUrl = "http://recorder:8090/source/usgs/earthquakes?run_id=ddb24389_1&attempt=0"
def sinkUrl = "http://recorder:8090/sink/ddb24389_1?attempt=0"

def slurper = new JsonSlurper()
def sourceJson = slurper.parse(new URL(sourceUrl).openConnection().inputStream)

def records = []
sourceJson.features.each { feature ->
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

def sinkJson = [records: records]
def jsonOutput = JsonOutput.toJson(sinkJson)

def connection = new URL(sinkUrl).openConnection()
connection.setRequestMethod("POST")
connection.setDoOutput(true)
connection.setRequestProperty("Content-Type", "application/json")
connection.outputStream.write(jsonOutput.bytes)

def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
