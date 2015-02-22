package influxdb_test

import (
	"encoding/json"
	"testing"

	"github.com/influxdb/influxdb/client"
)

// Ensure that data with epoch timestamps can be decoded.
func TestBatchPoints_Normal(t *testing.T) {
	var p client.BatchPoints
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
            "values": {
                    "value": 4541770385657154000
            }
        },
        {
            "name": "cpu",
             "tags": {
                "host": "server01"
            },
            "timestamp": 14244733039069380,
            "precision": "n",
            "values": {
                    "value": 7199311900554737000
            }
        }
    ]
}
`)

	if err := json.Unmarshal(data, &p); err != nil {
		t.Errorf("failed to unmarshal nanosecond data: %s", err.Error())
	}
}
