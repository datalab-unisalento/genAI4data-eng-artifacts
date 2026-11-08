import groovy.json.JsonSlurper
import groovy.json.JsonOutput

def source_url = "http://recorder:8090/source/usgs/earthquakes?run_id=6efdd80a_1&attempt=1"
def sink_url = "http://recorder:8090/sink/6efdd80a_1?attempt=1"

def connection = new URL(source_url).openConnection()
def jsonText = connection.getInputStream().getText("UTF-8")
def jsonSlurper = new JsonSlurper()
def geoJson = jsonSlurper.parseText(jsonText)

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
        def record = summary[band.name]
        record.event_count += 1
        record.max_magnitude = Math.max(record.max_magnitude, mag)
        record.max_depth_km = Math.max(record.max_depth_km, depth)
    }
}

def records = summary.values().findAll { it.event_count > 0 }

def postBody = JsonOutput.toJson(records)

def sinkConnection = new URL(sink_url).openConnection()
sinkConnection.setRequestMethod("POST")
sinkConnection.setDoOutput(true)
sinkConnection.setRequestProperty("Content-Type", "application/json")

def writer = new OutputStreamWriter(sinkConnection.getOutputStream())
writer.write(postBody)
writer.flush()
writer.close()

sinkConnection.getInputStream().close()

def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
