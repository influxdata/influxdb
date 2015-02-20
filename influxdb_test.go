package influxdb_test

import (
	"encoding/json"
	"testing"

	"github.com/influxdb/influxdb"
)

// Ensure the server can be successfully opened and closed.
func TestBatchPoints_Normal(t *testing.T) {
	var p influxdb.BatchPoints
	data := []byte(`
{                                                                                                                                                       
    "database": "foo",                                                                                                                                    
    "retentionPolicy": "bar",                                                                                                                              
    "points": [                                                                                                                                            
        {                                                                                                                                                   
            "name": "cpu",                                                                                                                                   
            "tags": {                                                                                                                                         
                "host": "server01"                                                                                                                            
            },
            "timestamp": 14244733039069373,
            "precision": "n",
            "values": [
                {
                    "value": 4541770385657154000
                }
            ]
        },
        {
            "name": "cpu",
             "tags": {
                "host": "server01"
            },
            "timestamp": 14244733039069380,
            "precision": "n",
            "values": [
                {
                    "value": 7199311900554737000
                }
            ]
        }
    ]
}
`)

	if err := json.Unmarshal(data, &p); err != nil {
		t.Error("failed to unmarshal nanosecond data")
	}
}
