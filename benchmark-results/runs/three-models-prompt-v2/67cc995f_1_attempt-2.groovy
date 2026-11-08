// Groovy script for ExecuteGroovyScript processor
// This script retrieves Open-Meteo particulate and gas data, converts hourly columns into records,
// inner-joins on exact time, and posts matching records to the HTTP sink.

import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.net.URL
import java.net.HttpURLConnection

// Source URLs from contract
def particulateUrl = "http://recorder:8090/source/open-meteo/particulate?run_id=67cc995f_1&attempt=2"
def gasesUrl = "http://recorder:8090/source/open-meteo/gases?run_id=67cc995f_1&attempt=2"

// Sink URL from contract
def sinkUrl = "http://recorder:8090/sink/67cc995f_1?attempt=2"

try {
    // Fetch particulate source
    def particulateConn = new URL(particulateUrl).openConnection()
    particulateConn.requestMethod = "GET"
    particulateConn.connect()
    def particulateText = particulateConn.getInputStream().text
    def particulateData = new JsonSlurper().parse(particulateText as String)
    
    // Fetch gases source
    def gasesConn = new URL(gasesUrl).openConnection()
    gasesConn.requestMethod = "GET"
    gasesConn.connect()
    def gasesText = gasesConn.getInputStream().text
    def gasesData = new JsonSlurper().parse(gasesText as String)
    
    // Extract hourly arrays from particulate
    def pTimes = particulateData.hourly.time as List<String>
    def pPm10 = particulateData.hourly.pm10 as List<Number>
    def pPm25 = particulateData.hourly.pm2_5 as List<Number>
    
    // Extract hourly arrays from gases
    def gTimes = gasesData.hourly.time as List<String>
    def gNitrogenDioxide = gasesData.hourly.nitrogen_dioxide as List<Number>
    def gOzone = gasesData.hourly.ozone as List<Number>
    
    // Build time-to-index maps for both datasets
    def pTimeIndex = [:]
    pTimes.eachWithIndex { time, idx -> pTimeIndex[time] = idx }
    
    def gTimeIndex = [:]
    gTimes.eachWithIndex { time, idx -> gTimeIndex[time] = idx }
    
    // Find common times (inner join on exact time values)
    def commonTimes = pTimeIndex.keySet().intersect(gTimeIndex.keySet())
    
    // Build matching records
    def records = []
    commonTimes.each { time ->
        def pIdx = pTimeIndex[time]
        def gIdx = gTimeIndex[time]
        records.add([
            time: time,
            pm10: pPm10[pIdx],
            pm2_5: pPm25[pIdx],
            nitrogen_dioxide: gNitrogenDioxide[gIdx],
            ozone: gOzone[gIdx]
        ])
    }
    
    // Build sink body as JSON array
    def sinkBody = JsonOutput.toJson(records)
    
    // POST to sink URL
    def sinkConn = new URL(sinkUrl).openConnection()
    sinkConn.requestMethod = "POST"
    sinkConn.setRequestProperty("Content-Type", "application/json")
    sinkConn.doOutput = true
    sinkConn.outputStream.withWriter("UTF-8") { writer ->
        writer.write(sinkBody)
    }
    // Read response to ensure request is sent
    def responseCode = sinkConn.getResponseCode()
    if (responseCode >= 300) {
        throw new RuntimeException("Sink returned HTTP status: $responseCode")
    }
    
    // Create and transfer FlowFile on success
    def flowFile = session.create()
    session.transfer(flowFile, REL_SUCCESS)
    
} catch (e) {
    // Re-throw to signal processor error; NiFi treats uncaught exceptions as errors
    throw e
}
