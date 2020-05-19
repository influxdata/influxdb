// This file is an adaptation of https://github.com/markusthoemmes/goautoneg.
// The copyright and license header are reproduced below.
//
//    Copyright [yyyy] [name of copyright owner]
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//
//    http://www.apache.org/licenses/LICENSE-2.0

package httpd

import (
	"mime"
	"sort"
	"strconv"
	"strings"
)

// accept is a structure to represent a clause in an HTTP Accept Header.
type accept struct {
	Type, SubType string
	Q             float64
	Params        map[string]string
}

// parseAccept parses the given string as an Accept header as defined in
// https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.1.
// Some rules are only loosely applied and might not be as strict as defined in the RFC.
func parseAccept(headers []string) []accept {
	var res []accept
	for _, header := range headers {
		parts := strings.Split(header, ",")
		for _, part := range parts {
			mt, params, err := mime.ParseMediaType(part)
			if err != nil {
				continue
			}

			accept := accept{
				Q:      1.0, // "[...] The default value is q=1"
				Params: params,
			}

			// A media-type is defined as
			// "*/*" | ( type "/" "*" ) | ( type "/" subtype )
			types := strings.Split(mt, "/")
			switch {
			// This case is not defined in the spec keep it to mimic the original code.
			case len(types) == 1 && types[0] == "*":
				accept.Type = "*"
				accept.SubType = "*"
			case len(types) == 2:
				accept.Type = types[0]
				accept.SubType = types[1]
			default:
				continue
			}

			if qVal, ok := params["q"]; ok {
				// A parsing failure will set Q to 0.
				accept.Q, _ = strconv.ParseFloat(qVal, 64)
				delete(params, "q")
			}

			res = append(res, accept)
		}
	}
	sort.SliceStable(res, func(i, j int) bool {
		return res[i].Q > res[j].Q
	})
	return res
}
