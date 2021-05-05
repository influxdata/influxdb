package transport

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform"
	notebooks "github.com/influxdata/influxdb/v2/notebooks/service"
)

// these functions are for generating demo data for development purposes.

func demoNotebook(orgID, notebookID platform.ID) notebooks.Notebook {
	return notebooks.Notebook{
		OrgID:     orgID,
		ID:        notebookID,
		Name:      "demo notebook",
		Spec:      demoSpec(1),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
}

func demoNotebooks(n int, orgID platform.ID) []notebooks.Notebook {
	o := []notebooks.Notebook{}

	for i := 1; i <= n; i++ {
		id, _ := platform.IDFromString(strconv.Itoa(1000000000000000 + i))

		o = append(o, notebooks.Notebook{
			OrgID:     orgID,
			ID:        *id,
			Name:      fmt.Sprintf("demo notebook %d", i),
			Spec:      demoSpec(i),
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		})
	}

	return o
}

func demoSpec(n int) map[string]interface{} {
	s := map[string]interface{}{}
	json.Unmarshal([]byte(fmt.Sprintf(demoSpecBlob, n)), &s)
	return s
}

const demoSpecBlob = `
{
	"name":"demo notebook %d",
	"pipes":[
			{
				"aggregateFunction":{
						"name":"mean"
				},
				"field":"",
				"measurement":"",
				"tags":{
						
				},
				"title":"Select a Metric",
				"type":"metricSelector",
				"visible":true
			},
			{
				"functions":[
						{
							"name":"mean"
						}
				],
				"panelHeight":200,
				"panelVisibility":"visible",
				"period":"10s",
				"properties":{
						"axes":{
							"x":{
									"base":"10",
									"bounds":[
										"",
										""
									],
									"label":"",
									"prefix":"",
									"scale":"linear",
									"suffix":""
							},
							"y":{
									"base":"10",
									"bounds":[
										"",
										""
									],
									"label":"",
									"prefix":"",
									"scale":"linear",
									"suffix":""
							}
						},
						"colors":[
							{
									"hex":"#31C0F6",
									"id":"c1f3c9a6-3404-4418-a43b-266a91da6790",
									"name":"Nineteen Eighty Four",
									"type":"scale",
									"value":0
							},
							{
									"hex":"#A500A5",
									"id":"be814008-8f22-4f50-a96a-f8d076b93dff",
									"name":"Nineteen Eighty Four",
									"type":"scale",
									"value":0
							},
							{
									"hex":"#FF7E27",
									"id":"9e5f2432-fcd8-4eac-9952-b26bb951fd8d",
									"name":"Nineteen Eighty Four",
									"type":"scale",
									"value":0
							}
						],
						"generateXAxisTicks":[
							
						],
						"generateYAxisTicks":[
							
						],
						"geom":"line",
						"hoverDimension":"auto",
						"legendOpacity":1,
						"legendOrientationThreshold":100000000,
						"note":"",
						"position":"overlaid",
						"queries":[
							{
									"builderConfig":{
										"aggregateWindow":{
												"fillValues":false,
												"period":"auto"
										},
										"buckets":[
												
										],
										"functions":[
												{
													"name":"mean"
												}
										],
										"tags":[
												{
													"aggregateFunctionType":"filter",
													"key":"_measurement",
													"values":[
															
													]
												}
										]
									},
									"editMode":"builder",
									"name":"",
									"text":""
							}
						],
						"shape":"chronograf-v2",
						"showNoteWhenEmpty":false,
						"type":"xy",
						"xColumn":null,
						"xTickStart":null,
						"xTickStep":null,
						"xTotalTicks":null,
						"yColumn":null,
						"yTickStart":null,
						"yTickStep":null,
						"yTotalTicks":null
				},
				"title":"Visualize the Result",
				"type":"visualization",
				"visible":true
			}
	],
	"range":{
			"duration":"1h",
			"label":"Past 1h",
			"lower":"now() - 1h",
			"seconds":3600,
			"type":"selectable-duration",
			"upper":null,
			"windowPeriod":10000
	},
	"readOnly":false,
	"refresh":{
			"interval":0,
			"status":"paused"
	}
}
`
