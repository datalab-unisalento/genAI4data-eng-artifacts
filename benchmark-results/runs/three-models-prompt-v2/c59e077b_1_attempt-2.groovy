def sourceUrl = "http://recorder:8090/source/open-meteo/particulate?run_id=c59e077b_1&attempt=2"
def sinkUrl = "http://recorder:8090/sink/c59e077b_1?attempt=2"

def http = new URL(sourceUrl).openConnection()
http.requestMethod = "GET"
def responseText = http.getInputStream().text
def data = new groovy.json.JsonSlurper().parse(responseText)
def times = data.hourly.time as List<String>
def pm25s = data.hourly.pm2_5 as List<Number>

for (int i = 2; i < times.size(); i++) {
    def window = pm25s[i-2..i] as List<Number>
    def maxVal = window.max()
    def record = [
        time: times[i],
        pm2_5: pm25s[i],
        pm2_5_peak_3h: maxVal
    ]
    def body = new groovy.json.JsonOutput().serialize([records: [record]])
    def sinkConn = new URL(sinkUrl).openConnection()
    sinkConn.setRequestMethod("POST")
    sinkConn.setDoOutput(true)
    sinkConn.setRequestProperty("Content-Type", "application/json")
    sinkConn.outputStream.withWriter("utf-8") { it.write(body.getBytes("utf-8")) }
    def respCode = sinkConn.responseCode
    if (respCode >= 300) {
        throw new RuntimeException("Sink returned status $respCode")
    }
}

def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
