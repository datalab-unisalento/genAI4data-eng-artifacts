def sourceUrl = 'http://recorder:8090/source/open-meteo/particulate?run_id=14fdd9d0_1&attempt=1'
def sinkUrl = 'http://recorder:8090/sink/14fdd9d0_1?attempt=1'

def response = new URL(sourceUrl).openConnection().with {
    it.connect()
    it.getInputStream().use { text(it.text) }
}
def data = new groovy.json.JsonSlurper().parse(text)

def records = []
if (data.hourly && data.hourly.time && data.hourly.pm10 && data.hourly.pm2_5) {
    def times = data.hourly.time
    def pm10s = data.hourly.pm10
    def pm25s = data.hourly.pm2_5
    for (int i = 0; i < times.size(); i++) {
        records << [
            time: times[i],
            pm10: pm10s[i],
            pm2_5: pm25s[i]
        ]
    }
}

def body = groovy.json.JsonOutput.toJson(records)
def conn = new URL(sinkUrl).openConnection()
conn.setRequestMethod('POST')
conn.setDoOutput(true)
conn.setRequestProperty('Content-Type', 'application/json')
conn.outputStream.with { out -> out.write(body.bytes) }
conn.responseCode
if (conn.responseCode >= 300) {
    throw new RuntimeException("HTTP ${conn.responseCode}")
}
def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
