curl 'http://localhost:9200/_template/current_template' -d '@indices_template.json'
curl 'http://localhost:9200/cmwell_current' -d '@FTSIndices.json'
curl 'http://localhost:9200/cmwell_history' -d '@FTSIndices.json'

