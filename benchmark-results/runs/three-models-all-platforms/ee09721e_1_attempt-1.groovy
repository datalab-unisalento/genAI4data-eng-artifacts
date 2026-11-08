import groovy.json.JsonSlurper
import groovy.json.JsonOutput

def slurper = new JsonSlurper()

// Fetch and parse particulate data
def particulateUrl = 'http://recorder:8090/source/open-meteo/particulate?run_id=ee09721e_1&attempt=1'
def particulateConn = new URL(particulateUrl).openConnection()
particulateConn.connectTimeout = 5000
particulateConn.readTimeout = 5000
def particulateData = slurper.parse(particulateConn.inputStream)

// Fetch and parse gases data
def gasesUrl = 'http://recorder:8090/source/open-meteo/gases?run_id=ee09721e_1&attempt=1'
def gasesConn = new URL(gasesUrl).openConnection()
gasesConn.connectTimeout = 5000
gasesConn.readTimeout = 5000
def gasesData = slurper.parse(gasesConn.inputStream)

// Convert hourly data to records for particulate
def particulateRecords = []
particulateData.hourly.time.eachWithIndex { time, index ->
    def record = [
        time: time,
        pm10: particulateData.hourly.pm10[index],
        pm2_5: particulateData.hourly.pm2_5[index]
    ]
    particulateRecords << record
}

// Convert hourly data to records for gases
def gasesRecords = []
gasesData.hourly.time.eachWithIndex { time, index ->
    def record = [
        time: time,
        nitrogen_dioxide: gasesData.hourly.nitrogen_dioxide[index],
        ozone: gasesData.hourly.ozone[index]
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
def sinkUrl = 'http://recorder:8090/sink/ee09721e_1?attempt=1'
def jsonBody = JsonOutput.toJson([records: joinedRecords])
def sinkConn = new URL(sinkUrl).openConnection()
sinkConn.setRequestMethod('POST')
sinkConn.setDoOutput(true)
sinkConn.setRequestProperty('Content-Type', 'application/json')
sinkConn.outputStream.write(jsonBody.bytes)

// Create and transfer FlowFile
def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
