package main

import (
	"encoding/json"
	"fmt"

	"github.com/loicalleyne/map2arrow"
)

func main() {
	m := make(map[string]any)
	err := json.Unmarshal([]byte(jsonS), &m)
	if err != nil {
		panic(err)
	}
	schema := map2arrow.Map2Arrow(m)
	fmt.Println(schema.String())
}

var jsonS string = `{
	"count": 89,
	"next": "https://sub.domain.com/api/search/?models=thurblig",
	"previous": null,
	"results": [
	  {
		"id": 6328,
		"name": "New user SMB check 2310-1",
		"external_id": null,
		"title": "New user SMB check 2310-1",
		"content_type": "new agent",
		"model": "Agent",
		"data": {
		  "id": 6328,
		  "dsp": {
			"id": 116,
			"name": "El Thingy Bueno",
			"nullarray":[]
		  },
		  "name": "New user SMB check 2310-1",
		  "agency": {
			"id": 925,
			"name": "New user SMB check 2310-1"
		  },
		  "export_status": {
			"status": true
		  }
		}
	  }
	]
  }`
