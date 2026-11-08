import groovy.json.JsonSlurper
import groovy.json.JsonOutput

def source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=02dcb91d_1&attempt=0"
def sink_url = "http://recorder:8090/sink/02dcb91d_1?attempt=0"

def connection = new URL(source_url).openConnection()
def jsonSlurper = new JsonSlurper()
def responseBody = connection.inputStream.text
def geoJson = jsonSlurper.parseText(responseBody)

def bands = [
    [name: "minor", min: Double.NEGATIVE_INFINITY, max: 4.0],
    [name: "moderate", min: 4.0, max: 5.0],
    [name: "major", min: 5.0, max: Double.POSITIVE_INFINITY]
]

def summary = [:]

bands.each { band ->
    summary[band.name] = [
        magnitude_band: band.name,
        event_count: 0,
        max_magnitude: Double.NEGATIVE_INFINITY,
        max_depth_km: Double.NEGATIVE_INFINITY
    ]
}

geoJson.features.each { feature ->
    def mag = feature.properties.mag
    def depth = feature.geometry.coordinates[2]

    def band = bands.find { band ->
        mag >= band.min && (mag < band.max || band.max == Double.POSITIVE_INFINITY)
    }

    if (band) {
        def summaryEntry = summary[band.name]
        summaryEntry.event_count += 1
        summaryEntry.max_magnitude = Math.max(summaryEntry.max_magnitude, mag)
        summaryEntry.max_depth_km = Math.max(summaryEntry.max_depth_km, depth)
    }
}

def records = summary.values().findAll { it.event_count > 0 }

def sinkConnection = new URL(sink_url).openConnection()
sinkConnection.setRequestMethod("POST")
sinkConnection.setDoOutput(true)
sinkConnection.setRequestProperty("Content-Type", "application/json")

def jsonBody = JsonOutput.toJson(records)
sinkConnection.outputStream.write(jsonBody.getBytes("UTF-8"))
sinkConnection.outputStream.close()

def responseCode = sinkConnection.responseCode
if (responseCode >= 300) {
    throw new RuntimeException("HTTP error code: ${responseCode}")
}

def session = context.session
def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
