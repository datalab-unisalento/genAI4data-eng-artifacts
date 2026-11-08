// Apache NiFi ExecuteGroovyScript source for task t1_open_meteo_ingestion

def sources = [
    "http://recorder:8090/source/open-meteo/particulate?run_id=91bde65e_1&attempt=0"
]
def sinkUrl = "http://recorder:8090/sink/91bde65e_1?attempt=0"

def slurper = new JsonSlurper()
def output = new JsonOutput()

def url = new URL(sources[0])
def conn = url.openConnection()
conn.setRequestMethod("GET")
def responseText = conn.getInputStream().text ?: conn.getInputStream().readBytes() as String
def data = slurper.parse(responseText)

def records = []
def times = data.hourly.time as List
def pm10s = data.hourly.pm10 as List
def pm25s = data.hourly.pm2_5 as List

times.eachWithIndex { time, idx ->
    def record = [
        "time": time,
        "pm10": pm10s[idx],
        "pm2_5": pm25s[idx]
    ]
    records.add(record)
}

def body = output.toJson(records)

def sinkConn = new URL(sinkUrl).openConnection()
sinkConn.setRequestMethod("POST")
sinkConn.setRequestProperty("Content-Type", "application/json")
sinkConn.getOutputStream().write(body.getBytes("UTF-8"))
sinkConn.getInputStream().close()

def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
