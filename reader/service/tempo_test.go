package service

import (
	"fmt"
	"testing"
)

func TestOTLPToJSON(t *testing.T) {
	str := `{
  "traceId": "BmnnjReqJcwSMLIHoMytSg==",
  "spanId": "NywoCfe0bLc=",
  "name": "test_span",
  "kind": 1,
  "startTimeUnixNano": "1734436231582466048",
  "endTimeUnixNano": "1734436231683010560",
  "attributes": [
    {
      "key": "testId",
      "value": {
        "stringValue": "__TEST__"
      }
    },
    {
      "key": "service.name",
      "value": {
        "stringValue": "testSvc"
      }
    },
    {
      "key": "telemetry.sdk.language",
      "value": {
        "stringValue": "nodejs"
      }
    },
    {
      "key": "telemetry.sdk.name",
      "value": {
        "stringValue": "opentelemetry"
      }
    },
    {
      "key": "telemetry.sdk.version",
      "value": {
        "stringValue": "0.25.0"
      }
    }
  ],
  "droppedAttributesCount": 0,
  "events": [
    {
      "timeUnixNano": "1734436231681999872",
      "name": "test event",
      "droppedAttributesCount": 0
    }
  ],
  "droppedEventsCount": 0,
  "droppedLinksCount": 0,
  "status": {
    "code": 1
  }
}`
	span, err := parseOTLPJson(&zipkinPayload{
		payload:     str,
		startTimeNs: 0,
		durationNs:  0,
		traceId:     "",
		spanId:      "",
		payloadType: 0,
		parentId:    "",
	})
	if err != nil {
		t.Error(err)
	}
	fmt.Println(span)
}
