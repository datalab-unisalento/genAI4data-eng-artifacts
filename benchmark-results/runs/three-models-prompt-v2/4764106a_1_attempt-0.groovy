import groovy.json.JsonSlurper
import groovy.json.JsonOutput

def slurper = new JsonSlurper()

// Fetch and parse particulate data
def particulateUrl = 'http://recorder:8090/source/open-meteo/particulate?run_id=4764106a_1&attempt=0'
def particulateConn = new URL(particulateUrl).openConnection()
particulateConn.connectTimeout = 5000
particulateConn.readTimeout = 5000
def particulateJson = slurper.parse(particulateConn.inputStream)

// Fetch and parse gases data
def gasesUrl = 'http://recorder:8090/source/open-meteo/gases?run_id=4764106a_1&attempt=0'
def gasesConn = new URL(gasesUrl).openConnection()
gasesConn.connectTimeout = 5000
gasesConn.readTimeout = 5000
def gasesJson = slurper.parse(gasesConn.inputStream)

// Convert hourly data to records for particulate
def particulateRecords = []
particulateJson.hourly.time.eachWithIndex { time, index ->
    def record = [
        time: time,
        pm10: particulateJson.hourly.pm10[index],
        pm2_5: particulateJson.hourly.pm2_5[index]
    ]
    particulateRecords << record
}

// Convert hourly data to records for gases
def gasesRecords = []
gasesJson.hourly.time.eachWithIndex { time, index ->
    def record = [
        time: time,
        nitrogen_dioxide: gasesJson.hourly.nitrogen_dioxide[index],
        ozone: gasesJson.hourly.ozone[index]
    ]
    gasesRecords << record
}

// Create maps for efficient joining
def particulateMap = particulateRecords.collectEntries { [it.time, it] }
def gasesMap = gasesRecords.collectEntries { [it.time, it] }

// Inner join on time
def joinedRecords = []
particulateMap.keySet().each { time ->
    if (gasesMap.containsKey(time)) {
        def record = [
            time: time,
            pm10: particulateMap[time].pm10,
            pm2_5: particulateMap[time].pm2_5,
            nitrogen_dioxide: gasesMap[time].nitrogen_dioxide,
            ozone: gasesMap[time].ozone
        ]
        joinedRecords << record
    }
}

// Prepare and send POST request to sink
def sinkUrl = 'http://recorder:8090/sink/4764106a_1?attempt=0'
def connection = new URL(sinkUrl).openConnection()
connection.setRequestMethod('POST')
connection.setDoOutput(true)
connection.setRequestProperty('Content-Type', 'application/json')

def jsonBody = JsonOutput.toJson([records: joinedRecords])
connection.outputStream.write(jsonBody.bytes)

// Read response to ensure body is sent
int responseCode = connection.responseCode
if (responseCode >= 300) {
    throw new Exception("Sink request failed with status code ${responseCode}")
}

def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
