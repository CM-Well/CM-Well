curl 'http://localhost:9200/_template/current_template' -d '@indices_template_new.json'
curl 'http://localhost:9200/cm_well_0' -d '@FTSIndices.json'

