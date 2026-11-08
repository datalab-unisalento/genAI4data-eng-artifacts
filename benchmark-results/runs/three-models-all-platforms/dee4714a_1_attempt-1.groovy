/*** Apache NiFi ExecuteGroovyScript body for task t3_open_meteo_join ***/
def sources = [
    "open_meteo_particulate": "http://recorder:8090/source/open-meteo/particulate?run_id=dee4714a_1&attempt=1",
    "open_meteo_gases": "http://recorder:8090/source/open-meteo/gases?run_id=dee4714a_1&attempt=1"
]
def sinkUrl = "http://recorder:8090/sink/dee4714a_1?attempt=1"

import groovy.json.JsonSlurper
import groovy.json.JsonOutput

def slurper = new JsonSlurper()

// Fetch and parse each source
def particulateData = null
def gasesData = null
try {
    def conn1 = new URL(sources["open_meteo_particulate"]).openConnection()
    particulateData = slurper.parseText(conn1.getInputStream().text)
} catch (Exception e) { throw e }
try {
    def conn2 = new URL(sources["open_meteo_gases"]).openConnection()
    gasesData = slurper.parseText(conn2.getInputStream().text)
} catch (Exception e) { throw e }

// Convert hourly arrays to record lists with time as key
def particulateRecords = []
if (particulateData && particulateData.hourly) {
    def times = particulateData.hourly.time
    def pm10s = particulateData.hourly.pm10
    def pm25s = particulateData.hourly.pm2_5
    for (int i = 0; i < times.size(); i++) {
        particulateRecords << [time: times[i], pm10: pm10s[i], pm2_5: pm25s[i]]
    }
}

def gasRecords = []
if (gasesData && gasesData.hourly) {
    def times = gasesData.hourly.time
    def no2s = gasesData.hourly.nitrogen_dioxide
    def ozons = gasesData.hourly.ozone
    for (int i = 0; i < times.size(); i++) {
        gasRecords << [time: times[i], nitrogen_dioxide: no2s[i], ozone: ozons[i]]
    }
}

// Inner join on exact time values
def partTimes = particulateRecords.collect { it.time }.toSet()
def gasTimes = gasRecords.collect { it.time }.toSet()
def commonTimes = partTimes.intersect(gasTimes)

def joinedRecords = []
commonTimes.each { t ->
    def p = particulateRecords.find { it.time == t }
    def g = gasRecords.find { it.time == t }
    if (p && g) {
        joinedRecords << [
            time: p.time,
            pm10: p.pm10,
            pm2_5: p.pm2_5,
            nitrogen_dioxide: g.nitrogen_dioxide,
            ozone: g.ozone
        ]
    }
}

// Deliver via HTTP POST
def sinkBody = joinedRecords ?: []
def postBody = new JsonOutput().toJson([records: sinkBody])
def sinkConn = new URL(sinkUrl).openConnection()
sinkConn.setRequestMethod("POST")
sinkConn.setRequestProperty("Content-Type", "application/json")
sinkConn.getOutputStream().write(postBody.bytes)
sinkConn.getInputStream().text // ensure connection completion

// Create success FlowFile and transfer
def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
