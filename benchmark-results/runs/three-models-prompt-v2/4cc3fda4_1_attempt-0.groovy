# Apache NiFi ExecuteGroovyScript Source

```groovy
import groovy.json.*
import java.net.URL
import java.net.HttpURLConnection

def sourceUrl = "http://recorder:8090/source/usgs/earthquakes?run_id=4cc3fda4_1&attempt=0"
def sinkUrl = "http://recorder:8090/sink/4cc3fda4_1?attempt=0"

def url = new URL(sourceUrl)
def connection = url.openConnection()
connection.requestMethod = "GET"
def responseText = connection.getInputStream().text as String

def slurper = new JsonSlurper()
def geoJson = slurper.parseText(responseText)

def features = geoJson.features as List
def records = []

features.each { feature ->
    def props = feature.properties
    def geom = feature.geometry
    def coords = geom.coordinates
    
    double magnitude = props.mag as Double
    String place = props.place as String
    long timeMs = props.time as Long
    double longitude = coords[0] as Double
    double latitude = coords[1] as Double
    double depthKm = coords[2] as Double
    
    String magnitudeBand
    if (magnitude < 4.0) {
        magnitudeBand = "minor"
    } else if (magnitude < 5.0) {
        magnitudeBand = "moderate"
    } else {
        magnitudeBand = "major"
    }
    
    def record = [
        id: feature.id as String,
        magnitude: magnitude,
        place: place,
        time_ms: timeMs,
        longitude: longitude,
        latitude: latitude,
        depth_km: depthKm,
        magnitude_band: magnitudeBand
    ]
    records.add(record)
}

def body = JsonOutput.toJson([records: records] as Object)

def sinkConn = new URL(sinkUrl).openConnection()
sinkConn.requestMethod = "POST"
sinkConn.setRequestProperty("Content-Type", "application/json")
sinkConn.doOutput = true
sinkConn.outputStream.withWriter("UTF-8") { writer ->
    writer.write(body)
}
def responseCode = sinkConn.responseCode
if (responseCode >= 300) {
    throw new RuntimeException("HTTP error: $responseCode")
}

def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
```
