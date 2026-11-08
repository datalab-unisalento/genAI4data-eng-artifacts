import groovy.json.JsonSlurper
import groovy.json.JsonOutput

def source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=e413f86c_1&attempt=1"
def sink_url = "http://recorder:8090/sink/e413f86c_1?attempt=1"

def connection = new URL(source_url).openConnection()
def jsonText = connection.getInputStream().getText("UTF-8")
def jsonSlurper = new JsonSlurper()
def data = jsonSlurper.parseText(jsonText)

def records = []

data.features.each { feature ->
    def record = [
        id: feature.id,
        magnitude: feature.properties.mag,
        place: feature.properties.place,
        time_ms: feature.properties.time,
        longitude: feature.geometry.coordinates[0],
        latitude: feature.geometry.coordinates[1],
        depth_km: feature.geometry.coordinates[2],
        magnitude_band: feature.properties.mag < 4.0 ? "minor" :
                       (feature.properties.mag < 5.0 ? "moderate" : "major")
    ]
    records.add(record)
}

def jsonOutput = JsonOutput.toJson(records)

def sinkConnection = new URL(sink_url).openConnection()
sinkConnection.setRequestMethod("POST")
sinkConnection.setDoOutput(true)
sinkConnection.setRequestProperty("Content-Type", "application/json")

def outputStream = sinkConnection.getOutputStream()
outputStream.write(jsonOutput.getBytes("UTF-8"))
outputStream.close()

def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
