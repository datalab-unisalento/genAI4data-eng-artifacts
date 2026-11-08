# Apache NiFi ExecuteGroovyScript - USGS Earthquake Band Summary

```groovy
import groovy.json.*
import java.net.URL
import java.net.HttpURLConnection

def sources = ["http://recorder:8090/source/usgs/earthquakes?run_id=1c8f1de4_1&attempt=0"]
def sinkUrl = "http://recorder:8090/sink/1c8f1de4_1?attempt=0"

def json = new URL(sources[0]).openConnection().with {
    it.setRequestMethod("GET")
    it.connect()
    it.getInputStream().use { reader -> reader.text }
}
def collection = new JsonSlurper().parse(json)
def features = collection.features

def bands = ["minor", "moderate", "major"]
def bandData = bands.collect { [band: it, events: [], maxMag: Double.NEGATIVE_INFINITY, maxDepth: Double.POSITIVE_INFINITY] as Map }

features.each { feature ->
    def mag = feature.properties.mag as double
    def depth = feature.geometry.coordinates[2] as double
    def band
    if (mag < 4.0) {
        band = "minor"
    } else if (mag < 5.0) {
        band = "moderate"
    } else {
        band = "major"
    }
    
    def bd = bandData.find { it.band == band }
    if (bd != null) {
        bd.events.add(feature)
        if (mag > bd.maxMag) bd.maxMag = mag
        if (depth < bd.maxDepth) bd.maxDepth = depth
    }
}

def records = bandData.collect { bd ->
    if (bd.events.isEmpty()) null else [
        magnitude_band: bd.band,
        event_count: bd.events.size(),
        max_magnitude: bd.maxMag,
        max_depth_km: bd.maxDepth
    ]
}.findAll { it != null }

def body = ["records": records] as JSON

def conn = new URL(sinkUrl).openConnection()
conn.setRequestMethod("POST")
conn.setDoOutput(true)
conn.setRequestProperty("Content-Type", "application/json")
conn.outputStream.with { os -> 
    os.write(new groovy.json.JsonOutput().serialize(body)) as void
}
conn.responseCode
if (conn.responseCode >= 300) {
    throw new Exception("Sink returned status: ${conn.responseCode}")
}

def session = ... // NiFi Session context provided by processor
def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
```
