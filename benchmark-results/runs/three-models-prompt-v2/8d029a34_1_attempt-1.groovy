import groovy.json.JsonSlurper
import groovy.json.JsonOutput

def process() {
    def sourceUrl = "http://recorder:8090/source/usgs/earthquakes?run_id=8d029a34_1&attempt=1"
    def sinkUrl = "http://recorder:8090/sink/8d029a34_1?attempt=1"

    // Fetch and parse source data
    def connection = new URL(sourceUrl).openConnection()
    def jsonText = connection.inputStream.text
    def earthquakes = new JsonSlurper().parseText(jsonText)

    // Initialize band statistics
    def bands = [
        minor: [count: 0, maxMag: -Float.MAX_VALUE, maxDepth: -Float.MAX_VALUE],
        moderate: [count: 0, maxMag: -Float.MAX_VALUE, maxDepth: -Float.MAX_VALUE],
        major: [count: 0, maxMag: -Float.MAX_VALUE, maxDepth: -Float.MAX_VALUE]
    ]

    // Process each earthquake
    earthquakes.features.each { feature ->
        def mag = feature.properties.mag as float
        def depth = feature.geometry.coordinates[2] as float

        if (mag < 4.0) {
            bands.minor.count++
            bands.minor.maxMag = Math.max(bands.minor.maxMag, mag)
            bands.minor.maxDepth = Math.max(bands.minor.maxDepth, depth)
        } else if (mag >= 4.0 && mag < 5.0) {
            bands.moderate.count++
            bands.moderate.maxMag = Math.max(bands.moderate.maxMag, mag)
            bands.moderate.maxDepth = Math.max(bands.moderate.maxDepth, depth)
        } else if (mag >= 5.0) {
            bands.major.count++
            bands.major.maxMag = Math.max(bands.major.maxMag, mag)
            bands.major.maxDepth = Math.max(bands.major.maxDepth, depth)
        }
    }

    // Build output records
    def records = []
    ['minor', 'moderate', 'major'].each { band ->
        if (bands[band].count > 0) {
            records << [
                magnitude_band: band,
                event_count: bands[band].count,
                max_magnitude: bands[band].maxMag,
                max_depth_km: bands[band].maxDepth
            ]
        }
    }

    // Send to sink
    def jsonOutput = JsonOutput.toJson([records: records])
    connection = new URL(sinkUrl).openConnection()
    connection.setRequestMethod("POST")
    connection.setDoOutput(true)
    connection.setRequestProperty("Content-Type", "application/json")

    connection.outputStream.withWriter { writer ->
        writer.write(jsonOutput)
    }

    def responseCode = connection.responseCode
    if (responseCode >= 300) {
        throw new Exception("Sink request failed with status: ${responseCode}")
    }

    // Create success flow file
    def flowFile = session.create()
    session.transfer(flowFile, REL_SUCCESS)
}

process()
