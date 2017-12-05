package influx

import (
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/chronograf"
)

func SortTemplates(ts []chronograf.TemplateVar) []chronograf.TemplateVar {
	sort.Slice(ts, func(i, j int) bool {
		if len(ts[i].Values) != len(ts[j].Values) {
			return len(ts[i].Values) < len(ts[j].Values)
		}

		if len(ts[i].Values) == 0 {
			return i < j
		}

		for k := range ts[i].Values {
			if ts[i].Values[k].Type != ts[j].Values[k].Type {
				return ts[i].Values[k].Type < ts[j].Values[k].Type
			}
			if ts[i].Values[k].Value != ts[j].Values[k].Value {
				return ts[i].Values[k].Value < ts[j].Values[k].Value
			}
		}
		return i < j
	})
	return ts
}

// RenderTemplate converts the template variable into a correct InfluxQL string based
// on its type
func RenderTemplate(query string, t chronograf.TemplateVar, now time.Time) (string, error) {
	if len(t.Values) == 0 {
		return query, nil
	}
	switch t.Values[0].Type {
	case "tagKey", "fieldKey", "measurement", "database":
		return strings.Replace(query, t.Var, `"`+t.Values[0].Value+`"`, -1), nil
	case "tagValue", "timeStamp":
		return strings.Replace(query, t.Var, `'`+t.Values[0].Value+`'`, -1), nil
	case "csv", "constant":
		return strings.Replace(query, t.Var, t.Values[0].Value, -1), nil
	}

	tv := map[string]string{}
	for i := range t.Values {
		tv[t.Values[i].Type] = t.Values[i].Value
	}

	if res, ok := tv["resolution"]; ok {
		resolution, err := strconv.ParseInt(res, 0, 64)
		if err != nil {
			return "", err
		}
		ppp, ok := tv["pointsPerPixel"]
		if !ok {
			ppp = "3"
		}
		pixelsPerPoint, err := strconv.ParseInt(ppp, 0, 64)
		if err != nil {
			return "", err
		}

		dur, err := ParseTime(query, now)
		if err != nil {
			return "", err
		}
		interval := AutoGroupBy(resolution, pixelsPerPoint, dur)
		return strings.Replace(query, t.Var, interval, -1), nil
	}
	return query, nil
}

func AutoGroupBy(resolution, pixelsPerPoint int64, duration time.Duration) string {
	// The function is: ((total_seconds * millisecond_converstion) / group_by) = pixels / 3
	// Number of points given the pixels
	pixels := float64(resolution) / float64(pixelsPerPoint)
	msPerPixel := float64(duration/time.Millisecond) / pixels
	secPerPixel := float64(duration/time.Second) / pixels
	if secPerPixel < 1.0 {
		if msPerPixel < 1.0 {
			msPerPixel = 1.0
		}
		return "time(" + strconv.FormatInt(int64(msPerPixel), 10) + "ms)"
	}
	// If groupby is more than 1 second round to the second
	return "time(" + strconv.FormatInt(int64(secPerPixel), 10) + "s)"
}

// TemplateReplace replaces templates with values within the query string
func TemplateReplace(query string, templates []chronograf.TemplateVar, now time.Time) (string, error) {
	templates = SortTemplates(templates)
	for i := range templates {
		var err error
		query, err = RenderTemplate(query, templates[i], now)
		if err != nil {
			return "", err
		}
	}
	return query, nil
}
