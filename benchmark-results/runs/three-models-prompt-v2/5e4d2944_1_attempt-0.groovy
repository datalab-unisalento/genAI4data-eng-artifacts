import groovy.json.JsonSlurper
import groovy.json.JsonOutput

def slurper = new JsonSlurper()

def particulateUrl = "http://recorder:8090/source/open-meteo/particulate?run_id=5e4d2944_1&attempt=0"
def gasesUrl = "http://recorder:8090/source/open-meteo/gases?run_id=5e4d2944_1&attempt=0"
def sinkUrl = "http://recorder:8090/sink/5e4d2944_1?attempt=0"

def particulateResponse = slurper.parse(particulateUrl.toURL().openConnection().getInputStream())
def gasesResponse = slurper.parse(gasesUrl.toURL().openConnection().getInputStream())

def particulateRecords = []
def gasesRecords = []

// Convert particulate hourly data to records
def pmTime = particulateResponse.hourly.time
def pm10Values = particulateResponse.hourly.pm10
def pm25Values = particulateResponse.hourly.pm2_5

for (int i = 0; i < pmTime.size(); i++) {
    particulateRecords.add([
        time: pmTime[i],
        pm10: pm10Values[i],
        pm2_5: pm25Values[i]
    ])
}

// Convert gases hourly data to records
def gasTime = gasesResponse.hourly.time
def no2Values = gasesResponse.hourly.nitrogen_dioxide
def ozoneValues = gasesResponse.hourly.ozone

for (int i = 0; i < gasTime.size(); i++) {
    gasesRecords.add([
        time: gasTime[i],
        nitrogen_dioxide: no2Values[i],
        ozone: ozoneValues[i]
    ])
}

// Inner join on exact time
def joinedRecords = []

particulateRecords.each { pmRecord ->
    def matchingGasRecord = gasesRecords.find { it.time == pmRecord.time }
    if (matchingGasRecord) {
        joinedRecords.add([
            time: pmRecord.time,
            pm10: pmRecord.pm10,
            pm2_5: pmRecord.pm2_5,
            nitrogen_dioxide: matchingGasRecord.nitrogen_dioxide,
            ozone: matchingGasRecord.ozone
        ])
    }
}

// Post to sink
def connection = sinkUrl.toURL().openConnection()
connection.setRequestMethod("POST")
connection.setDoOutput(true)
connection.setRequestProperty("Content-Type", "application/json")

def jsonBody = JsonOutput.toJson(joinedRecords)
connection.outputStream.write(jsonBody.getBytes("UTF-8"))
connection.outputStream.close()

int responseCode = connection.responseCode
if (responseCode >= 300) {
    throw new RuntimeException("HTTP error code: ${responseCode}")
}

// Create FlowFile for success
def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
