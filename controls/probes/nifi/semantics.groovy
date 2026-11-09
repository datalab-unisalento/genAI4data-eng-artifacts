import groovy.json.JsonOutput

def plain = [properties: [mag: 5.0], mag: 1.0]
def parsed = new groovy.json.JsonSlurper().parseText('{"properties": {"mag": 5.0}, "mag": 1.0}')
def checks = [
    [check: "plain_map_dot_properties_mag", value: String.valueOf(plain.properties?.mag)],
    [check: "parsed_map_dot_properties_mag", value: String.valueOf(parsed.properties?.mag)],
    [check: "parsed_map_bracket_properties_mag", value: String.valueOf(parsed['properties']?.get('mag'))],
    [check: "parsed_map_get_properties_mag", value: String.valueOf(parsed.get('properties')?.get('mag'))],
]

def connection = new URL("http://recorder:8090/sink/probe-semantics?attempt=0").openConnection()
connection.requestMethod = "POST"
connection.doOutput = true
connection.setRequestProperty("Content-Type", "application/json")
connection.outputStream.withWriter("UTF-8") { it << JsonOutput.toJson(checks) }
if (connection.responseCode >= 300) {
    throw new RuntimeException("sink returned HTTP " + connection.responseCode)
}

def flowFile = session.create()
session.transfer(flowFile, REL_SUCCESS)
