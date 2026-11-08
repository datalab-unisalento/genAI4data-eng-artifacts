// Groovy Script Body for ExecuteGroovyScript - t4_daily_particulate_summary

def sourceUrl = "http://recorder:8090/source/open-meteo/particulate?run_id=3eac1efa_1&attempt=0"
def sinkUrl = "http://recorder:8090/sink/3eac1efa_1?attempt=0"

def responseText = new URL(sourceUrl).openConnection().getInputStream.text
def data = new groovy.json.JsonSlurper().parse(responseText)

def hourly = data.hourly
def times = hourly.time as List<String>
def pm25 = hourly.pm2_5 as List<Number>
def pm10 = hourly.pm10 as List<Number>

def dateGroups = [:]
for (i in 0 until times.size()) {
    def date = times[i].substring(0, 10)
    if (!dateGroups.containsKey(date)) {
        dateGroups[date] = []
    }
    dateGroups[date].add([pm25: pm25[i], pm10: pm10[i]])
}

def records = dateGroups.entries.collect { date, entries ->
    def pm25Max = entries.max { it.pm25 } as Number
    def pm10Max = entries.max { it.pm10 } as Number
    [date: date, hours: entries.size(), pm2_5_max: pm25Max, pm10_max: pm10Max]
}

def body = groovy.json.JsonOutput.toJson([records: records])

def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS) as FlowFile
flowFile.write(body.getBytes())
flowFile = session.get()
flowFile = session.putAllHeaders([Content_Type: "application/json"])
flowFile = session.write(flowFile.outputStream) { os -> os.write(flowFile.content.toByteArray()) }
session.transfer(flowFile, REL_SUCCESS)
