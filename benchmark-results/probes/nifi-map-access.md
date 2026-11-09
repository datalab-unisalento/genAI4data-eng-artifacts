| Probe | Expression | Delivered rows | Non-null magnitude | Processor error |
|---|---|---|---|---|
| dot | `f.properties.mag` | 49 | 0 | — |
| bracket | `f.properties['mag']` | 49 | 0 | — |
| get_get | `f.get('properties').get('mag')` | 49 | 49 | — |
| get_dot | `f.get('properties').mag` | 49 | 49 | — |

Groovy semantics check (same NiFi processor):

| Check | Value |
|---|---|
| plain_map_dot_properties_mag | null |
| parsed_map_dot_properties_mag | null |
| parsed_map_bracket_properties_mag | null |
| parsed_map_get_properties_mag | 5.0 |
