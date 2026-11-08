import groovy.json.JsonSlurper
import groovy.json.JsonOutput

def url = new URL("http://recorder:8090/source/usgs/earthquakes?run_id=19a11a70_1&attempt=0")
def connection = url.openConnection()
def jsonText = connection.inputStream.text
def earthquakes = new JsonSlurper().parseText(jsonText)

def bands = [
    minor: [min: -Float.MAX_VALUE, max: 4.0],
    moderate: [min: 4.0, max: 5.0],
    major: [min: 5.0, max: Float.MAX_VALUE]
]

def summaries = [:].withDefault { band ->
    [
        magnitude_band: band,
        event_count: 0,
        max_magnitude: -Float.MAX_VALUE,
        max_depth_km: -Float.MAX_VALUE
    ]
}

earthquakes.features.each { feature ->
    def mag = feature.properties.mag as float
    def depth = feature.geometry.coordinates[2] as float
    
    bands.findResult { bandName, range ->
        if (mag >= range.min && mag < range.max) {
            summaries[bandName].event_count++
            summaries[bandName].max_magnitude = Math.max(summaries[bandName].max_magnitude, mag)
            summaries[bandName].max_depth_km = Math.max(summaries[bandName].max_depth_km, depth)
        }
    }
}

def result = summaries.values().findAll { it.event_count > 0 }

def sinkUrl = new URL("http://recorder:8090/sink/19a11a70_1?attempt=0")
def connection2 = sinkUrl.openConnection()
connection2.setRequestMethod('POST')
connection2.setDoOutput(true)
connection2.setRequestProperty('Content-Type', 'application/json')

def jsonBody = JsonOutput.toJson(result)
connection2.outputStream.write(jsonBody.bytes)

flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
