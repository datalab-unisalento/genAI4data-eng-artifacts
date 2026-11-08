from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlencode

# Bump when the prompt wording changes, and record it in every run file.
PROMPT_VERSION = "v2"


def build_prompt(
    task_id: str,
    task: dict[str, Any],
    benchmark: dict[str, Any],
    platform: str,
    recorder_url: str,
    run_id: str,
    attempt: int = 0,
    previous_artifact: str | None = None,
    failure_report: str | None = None,
) -> str:
    sources = {
        source_id: {
            "url": recorder_url.rstrip("/") + benchmark["sources"][source_id]["path"]
            + "?" + urlencode({"run_id": run_id, "attempt": attempt}),
            "description": benchmark["sources"][source_id]["description"],
            "response_schema": benchmark["sources"][source_id]["response_schema"],
        }
        for source_id in task["source_ids"]
    }
    contract = {
        "task_id": task_id,
        "sources": sources,
        "sink_url": f"{recorder_url.rstrip('/')}/sink/{run_id}?attempt={attempt}",
        "sink_body": "JSON array of record objects (or {\"records\": [...]})",
        "required_fields": task["required_fields"],
        "task_details": task["instructions"],
    }
    source_nodes = [
        {
            "parameters": {"method": "GET", "url": source["url"], "options": {}},
            "id": f"source_{index}",
            "name": f"Source {index}",
            "type": "n8n-nodes-base.httpRequest",
            "typeVersion": 4.2,
            "position": [300, (index - 1) * 180],
        }
        for index, source in enumerate(sources.values(), start=1)
    ]
    # With several sources, a Merge node waits for every source before the Code node runs.
    # Without it, the Code node can run before a later source has executed.
    merge_nodes = (
        [{
            "parameters": {"mode": "append", "numberInputs": len(source_nodes)},
            "id": "merge_sources",
            "name": "Merge Sources",
            "type": "n8n-nodes-base.merge",
            "typeVersion": 3,
            "position": [450, 90],
        }]
        if len(source_nodes) > 1 else []
    )
    n8n_skeleton = {
        "id": f"bench_{run_id}",
        "name": "Benchmark workflow",
        "active": False,
        "nodes": [
            {
                "parameters": {},
                "id": "benchmark_trigger",
                "name": "Benchmark Trigger",
                "type": "n8n-nodes-base.manualTrigger",
                "typeVersion": 1,
                "position": [0, 0],
            },
            *source_nodes,
            *merge_nodes,
            {
                "parameters": {
                    "mode": "runOnceForAllItems",
                    "jsCode": "const records = [];\nreturn [{ json: { records } }];",
                },
                "id": "transform",
                "name": "Transform",
                "type": "n8n-nodes-base.code",
                "typeVersion": 2,
                "position": [600, 0],
            },
            {
                "parameters": {
                    "method": "POST",
                    "url": contract["sink_url"],
                    "sendBody": True,
                    "specifyBody": "json",
                    "jsonBody": "={{ $json }}",
                    "options": {},
                },
                "id": "sink",
                "name": "Sink",
                "type": "n8n-nodes-base.httpRequest",
                "typeVersion": 4.2,
                "position": [900, 0],
            },
        ],
        "connections": {
            "Benchmark Trigger": {
                "main": [[
                    {
                        "node": f"Source {index}",
                        "type": "main",
                        "index": 0,
                    }
                    for index in range(1, len(source_nodes) + 1)
                ]]
            },
            **{
                f"Source {index}": {
                    "main": [[
                        {"node": "Merge Sources" if merge_nodes else "Transform", "type": "main",
                         "index": index - 1 if merge_nodes else 0}
                    ]]
                }
                for index in range(1, len(source_nodes) + 1)
            },
            **({"Merge Sources": {"main": [[{"node": "Transform", "type": "main", "index": 0}]]}}
               if merge_nodes else {}),
            "Transform": {"main": [[{"node": "Sink", "type": "main", "index": 0}]]},
        },
        "settings": {
            "executionOrder": "v1",
            "saveDataErrorExecution": "all",
            "saveDataSuccessExecution": "all",
            "saveManualExecutions": True,
        },
    }
    platform_instructions = {
        "airflow": (
            "Return only a complete Python Apache Airflow DAG file. Define exactly one DAG "
            f"with dag_id='bench_{run_id.replace('-', '_')}'. Use this structure: "
            "include the explicit imports `from airflow import DAG`, "
            "`from airflow.operators.python import PythonOperator`, and "
            "`from datetime import datetime`. "
            "`with DAG(dag_id=..., start_date=datetime(2024, 1, 1), schedule=None, "
            "catchup=False) as dag:` and declare a PythonOperator inside that context manager; "
            "do not call `dag.add`. The task callable must take no required arguments and must "
            "not suppress exceptions. Use the exact source and sink URLs from the current "
            "contract, not URLs copied from a previous attempt. Fetch each source using "
            "urllib.request.urlopen. To deliver output, construct "
            "`urllib.request.Request(sink_url, data=json.dumps(records).encode('utf-8'), "
            "headers={'Content-Type': 'application/json'}, method='POST')` and pass that "
            "request to `urlopen`. Include imports and all task logic in this single file. "
            "Do not access files, environment secrets, or network destinations other than the "
            "listed fixture URLs and sink."
        ),
        "dagster": (
            "Return one complete Python file. Import only `op` and `job` from Dagster, plus "
            "standard-library modules needed for JSON and HTTP. The file MUST start with the "
            "exact line `from dagster import op, job`. Define one zero-argument op using exactly "
            "`@op` (not `@op()`), with no decorator arguments; inside it, use the literal current "
            "source and sink URLs from the contract, fetch the source with urllib.request, "
            "transform the records exactly as specified, and POST them to the sink using "
            "urllib.request.Request with a JSON body and Content-Type application/json. Do not "
            "use Dagster context, resources, op inputs, output schemas, or output mappings. "
            "Then include exactly this job declaration, with no decorator arguments:\n"
            f"`@job\\ndef bench_{run_id.replace('-', '_')}():\\n    ingest_pipeline()`\n"
            "The op function must be named `ingest_pipeline`. Do not suppress exceptions or access any "
            "other network destination. The job must be loadable by "
            "`dagster job execute -f FILE -j "
            f"bench_{run_id.replace('-', '_')}`."
        ),
        "prefect": (
            "Return one complete Python file defining a Prefect flow named "
            f"`bench_{run_id.replace('-', '_')}` with `@flow`. Include the explicit import "
            "`from prefect import flow`; do not import any other Prefect symbols. "
            "Implement the task inside the flow; "
            "use only the current fixture source URLs and HTTP sink URL from the contract. Fetch "
            "each source with `urllib.request.urlopen(source_url)`, transform rows exactly as "
            "specified, then create `urllib.request.Request(sink_url, "
            "data=json.dumps(records).encode('utf-8'), "
            "headers={'Content-Type': 'application/json'}, method='POST')` and pass that "
            "request to `urllib.request.urlopen(request)`. Import `json` and `urllib.request`. "
            "Do not "
            "suppress exceptions or access any other network destination. Invoke the flow under "
            "`if __name__ == '__main__':` so the file executes it when run with Python."
        ),
        "nifi": (
            "Return only the Groovy source for the Script Body of an Apache NiFi "
            "ExecuteGroovyScript processor. NiFi runs the script once when the processor is started "
            "and treats any uncaught exception as a processor error. The script must fetch each "
            "source with `new URL(source_url).openConnection()` (or an equivalent java.net call) "
            "and read the body as text, parse it with `groovy.json.JsonSlurper`, transform the "
            "records exactly as specified, and deliver them with an HTTP POST to the sink URL. The "
            "POST body must be JSON with `Content-Type: application/json`, built with "
            "`groovy.json.JsonOutput`. Use the exact source and sink URLs from the current contract. "
            "Java's HttpURLConnection sends a request body only when the response is read, so after "
            "writing the body call `connection.responseCode` and throw an exception if the status is "
            "300 or higher. After successful delivery, create a FlowFile with `def flowFile = session.create()` "
            "and call `session.transfer(flowFile, REL_SUCCESS)`. Do not catch and suppress "
            "exceptions, and do not access any other network destination. Use only Groovy and the "
            "Java standard library; do not use NiFi controller services or other processors."
        ),
        "kestra": (
            "Return one complete Kestra flow in YAML. The flow must declare the top-level keys "
            f"`id: bench_{run_id.replace('-', '_')}` and `namespace: bench`, and must not define "
            "triggers or inputs. It must contain exactly one task of type "
            "`io.kestra.plugin.scripts.python.Script` with `runner: PROCESS` and a `script:` block "
            "written as a YAML literal block. Give the task an `id` key (not `task_id`), for example:\n"
            "tasks:\n  - id: ingest\n    type: io.kestra.plugin.scripts.python.Script\n    runner: PROCESS\n"
            "    script: |\n      import json\n      ...\n"
            "The Python script must fetch each source with "
            "`urllib.request.urlopen`, transform the records exactly as specified, and POST them to "
            "the sink with `urllib.request.Request(sink_url, data=json.dumps(records).encode('utf-8'), "
            "headers={'Content-Type': 'application/json'}, method='POST')`. Use the exact source and "
            "sink URLs from the current contract. Do not suppress exceptions, do not access any other "
            "network destination, and do not use the Kestra `{{` or `}}` template delimiters anywhere "
            "in the flow."
        ),
        "n8n": (
            "Return a complete n8n workflow JSON object. Start from the canonical workflow "
            "skeleton below and preserve its node names, IDs, types, settings, parameters, and "
            "connections exactly. Replace only the Transform node's `parameters.jsCode` with "
            "working JavaScript for the task. The skeleton already contains the current attempt's "
            "source and sink URLs; do not edit or reconstruct them. For two-source tasks, read "
            "each response by node name (for example, `$('Source 1').first().json`) and join or "
            "combine them as required. The Code node must return one n8n item shaped as "
            "`[{ json: { records: [...] } }]`. Do not add, remove, rename, or reconnect nodes. "
            "Return only the JSON object, without markdown fences.\n\n"
            "Canonical n8n workflow skeleton:\n"
            f"{json.dumps(n8n_skeleton, indent=2)}"
        ),
    }
    prompt = (
        "Task:\n"
        f"{task['prompt'].format(platform=platform)}\n\n"
        "Execution contract:\n"
        "Use only the supplied local fixture source URLs and HTTP sink URL; do not call the "
        "external source API or any other network destination. The source schema below describes "
        "record structure and types, not the saved fixture records. Implement the task without "
        "hardcoding fixture values. The sink is scored as a multiset of complete records, including "
        "exact field values, keys, and duplicate counts.\n\n"
        f"Required {platform} artifact: {platform_instructions[platform]}\n"
        "Return only the requested artifact, with no explanatory prose or markdown fences.\n\n"
        "Run-specific URLs and data contract:\n"
        f"{json.dumps(contract, indent=2)}\n"
    )
    if failure_report:
        prompt += (
            "\nYour previous artifact failed execution or evaluation. Correct it using this "
            "machine-derived report. Return a complete replacement artifact, not a patch. "
            "For n8n, rebuild from the canonical skeleton above; do not copy the previous "
            "artifact's structure or URLs. The current attempt's URLs are in the skeleton and "
            "contract.\n"
            f"Failure report:\n{failure_report}\n"
            f"Previous artifact:\n{previous_artifact or ''}\n"
        )
    return prompt
