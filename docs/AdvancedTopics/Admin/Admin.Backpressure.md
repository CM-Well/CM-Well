# _backpressure API

## Description

The **_backpressure** API allows admin users to control the backpressure of CM-Well's WS (Web Service) module. You may want to do this when very heavy ingest operations are filling the processing queue to capacity. The backpressure feature controls when users will get "Server is busy" (503) responses when they attempt to add an update request.


## Syntax

**URL:** ```<cm-well-host>/_backpressure```

**REST verb:** POST

**Mandatory parameters:** pbp

----------

**Template:**

```
    curl -X POST "<cm-well-host>/_backpressure?pbp=<backpressure mode>"
```

**URL example:** N/A

**Curl example (REST API):**

```
    curl -X POST "<cm-well-host>/_backpressure?pbp=enable" 
```

## Parameters

Parameter | Description | Values 
:----------|:-------------|:--------
pbp | Backpressure mode | **enable** - backpressure is enabled. Requests are rejected when the Kafka queue size reaches a configurable threshold (currently 500,000 messages); the caller gets an HTTP 503 error.<br/><br/>**disable** - backpressure is disabled. CM-Well won't intentionally reject user requests.<br/><br/>**block** - all ingest requests are automatically rejected with a 503 error. This can be helpful during a maintenance period, when you want to temporarily block all requests.

## Code Example

### Call

```
    curl -X POST "<cm-well-host>/_backpressure?pbp=enable"
```

### Results

```
    {"success":true}
```


