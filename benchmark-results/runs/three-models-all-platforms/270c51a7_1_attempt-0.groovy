import groovy.json.JsonSlurper
import groovy.json.JsonOutput

def source1Url = "http://recorder:8090/source/open-meteo/particulate?run_id=270c51a7_1&attempt=0"
def source2Url = "http://recorder:8090/source/open-meteo/gases?run_id=270c51a7_1&attempt=0"
def sinkUrl = "http://recorder:8090/sink/270c51a7_1?attempt=0"

def jsonSlurper = new JsonSlurper()
def source1Conn = new URL(source1Url).openConnection()
def source2Conn = new URL(source2Url).openConnection()

def source1Json = jsonSlurper.parse(source1Conn.getInputStream())
def source2Json = jsonSlurper.parse(source2Conn.getInputStream())

def particulateRecords = []
def gasesRecords = []

// Convert particulate data to records
def timeArray1 = source1Json.hourly.time
def pm10Array = source1Json.hourly.pm10
def pm25Array = source1Json.hourly.pm2_5

for (int i = 0; i < timeArray1.size(); i++) {
    particulateRecords.add([
        time: timeArray1[i],
        pm10: pm10Array[i],
        pm2_5: pm25Array[i]
    ])
}

// Convert gases data to records
def timeArray2 = source2Json.hourly.time
def no2Array = source2Json.hourly.nitrogen_dioxide
def ozoneArray = source2Json.hourly.ozone

for (int i = 0; i < timeArray2.size(); i++) {
    gasesRecords.add([
        time: timeArray2[i],
        nitrogen_dioxide: no2Array[i],
        ozone: ozoneArray[i]
    ])
}

// Inner join on time
def joinedRecords = []

particulateRecords.each { pRec ->
    def gRec = gasesRecords.find { it.time == pRec.time }
    if (gRec != null) {
        joinedRecords.add([
            time: pRec.time,
            pm10: pRec.pm10,
            pm2_5: pRec.pm2_5,
            nitrogen_dioxide: gRec.nitrogen_dioxide,
            ozone: gRec.ozone
        ])
    }
}

// Post to sink
if (!joinedRecords.isEmpty()) {
    def payload = JsonOutput.toJson(joinedRecords)
    
    def conn = new URL(sinkUrl).openConnection()
    conn.setRequestMethod("POST")
    conn.setDoOutput(true)
    conn.setRequestProperty("Content-Type", "application/json")

    def writer = new OutputStreamWriter(conn.getOutputStream())
    writer.write(payload)
    writer.flush()
    writer.close()

    conn.getInputStream().close()
}

def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
