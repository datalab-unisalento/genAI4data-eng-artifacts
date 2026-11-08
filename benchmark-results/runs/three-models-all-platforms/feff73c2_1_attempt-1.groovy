import groovy.json.*
import java.net.URL
import java.net.HttpURLConnection

def sources = ["http://recorder:8090/source/usgs/earthquakes?run_id=feff73c2_1&attempt=1"]
def sinkUrl = "http://recorder:8090/sink/feff73c2_1?attempt=1"

def features = []
sources.each { sourceUrl ->
    def conn = new URL(sourceUrl).openConnection()
    conn.requestMethod = "GET"
    conn.connectInput = true
    def reader = conn.getInputStream(new groovy.io.CharReader())
    def text = reader.use { it.readText() }
    def json = new JsonSlurper().parse(text)
    if (json && json.features) {
        features.addAll(json.features as List)
    }
}

def bands = ["minor", "moderate", "major"]
def bandData = bands.collect { [band: it, events: [], maxMag: Double.NEGATIVE_INFINITY, maxDepth: Double.POSITIVE_INFINITY] }

def eventMap = features.inject([:]) { map, feature ->
    def mag = feature.properties?.mag as double
    def depth = feature.geometry?.coordinates?.get(2) as double
    if (mag != null && depth != null) {
        String band
        if (mag < 4.0) band = "minor"
        else if (mag < 5.0) band = "moderate"
        else band = "major"
        
        def bd = bandData.find { it.band == band }
        if (bd) {
            bd.events.add(feature)
            if (mag > bd.maxMag) bd.maxMag = mag
            if (depth < bd.maxDepth) bd.maxDepth = depth
        }
    }
    map
}

def records = []
bandData.each { bd ->
    if (bd.events.size() > 0) {
        records.add([
            magnitude_band: bd.band,
            event_count: bd.events.size(),
            max_magnitude: bd.maxMag,
            max_depth_km: bd.maxDepth
        ])
    }
}

def payload = ["records": records as List]
def jsonOutput = new JsonOutput()
def postBody = jsonOutput.toJson(payload)

def sinkConn = new URL(sinkUrl).openConnection()
sinkConn.requestMethod = "POST"
sinkConn.setRequestProperty("Content-Type", "application/json")
sinkConn.doOutput = true
sinkConn.getOutputStream().write(postBody.bytes)
sinkConn.connect
sinkConn.getInputStream().close()

def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
