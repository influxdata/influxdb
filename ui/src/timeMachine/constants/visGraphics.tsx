import React from 'react'

import {ViewType} from 'src/types'
import {VIS_TYPES} from './index'

const GRAPHIC_SVGS = {
  heatmap: (
    <div className="vis-graphic" data-testid="vis-graphic--heatmap">
      <svg
        width="100%"
        height="100%"
        version="1.1"
        id="Histogram"
        x="0px"
        y="0px"
        viewBox="0 0 150 150"
        preserveAspectRatio="none meet"
      >
        <g id="r">
          <polygon
            className="vis-graphic--fill vis-graphic--fill-b"
            points="127.5,127.5 127.5,112.5 112.5,112.5 112.5,127.5 97.5,127.5 97.5,142.5 112.5,142.5 127.5,142.5
		142.5,142.5 142.5,127.5 	"
          />
          <polygon
            className="vis-graphic--fill vis-graphic--fill-b"
            points="67.5,127.5 52.5,127.5 52.5,112.5 37.5,112.5 37.5,97.5 22.5,97.5 22.5,112.5 22.5,127.5 7.5,127.5
		7.5,142.5 22.5,142.5 37.5,142.5 52.5,142.5 67.5,142.5 82.5,142.5 82.5,127.5 82.5,112.5 67.5,112.5 	"
          />
        </g>
        <g id="rl">
          <g>
            <path
              className="vis-graphic--fill vis-graphic--fill-b"
              d="M126,114v13.5v1.5h1.5H141v12h-13.5h-15H99v-12h13.5h1.5v-1.5V114H126 M127.5,112.5h-15v15h-15v15h15h15h15
			v-15h-15V112.5L127.5,112.5z"
            />
          </g>
          <g>
            <path
              className="vis-graphic--fill vis-graphic--fill-b"
              d="M36,99v13.5v1.5h1.5H51v13.5v1.5h1.5h15H69v-1.5V114h12v13.5V141H67.5h-15h-15h-15H9v-12h13.5H24v-1.5v-15V99
			H36 M37.5,97.5h-15v15v15h-15v15h15h15h15h15h15v-15v-15h-15v15h-15v-15h-15V97.5L37.5,97.5z"
            />
          </g>
        </g>
        <g id="g">
          <polygon
            className="vis-graphic--fill vis-graphic--fill-c"
            points="97.5,97.5 97.5,82.5 82.5,82.5 82.5,97.5 67.5,97.5 52.5,97.5 37.5,97.5 37.5,112.5 52.5,112.5
		52.5,127.5 67.5,127.5 67.5,112.5 82.5,112.5 82.5,127.5 82.5,142.5 97.5,142.5 97.5,127.5 112.5,127.5 112.5,112.5 97.5,112.5
		"
          />
          <polygon
            className="vis-graphic--fill vis-graphic--fill-c"
            points="127.5,82.5 127.5,97.5 112.5,97.5 112.5,112.5 127.5,112.5 127.5,127.5 142.5,127.5 142.5,112.5
		142.5,97.5 142.5,82.5 	"
          />
          <polygon
            className="vis-graphic--fill vis-graphic--fill-c"
            points="37.5,67.5 22.5,67.5 22.5,82.5 7.5,82.5 7.5,97.5 7.5,112.5 7.5,127.5 22.5,127.5 22.5,112.5
		22.5,97.5 37.5,97.5 37.5,82.5 	"
          />
        </g>
        <g id="gl">
          <g>
            <path
              className="vis-graphic--fill vis-graphic--fill-c"
              d="M96,84v13.5v15v1.5h1.5H111v12H97.5H96v1.5V141H84v-13.5v-15V111h-1.5h-15H66v1.5V126H54v-13.5V111h-1.5H39
			V99h13.5h15h15H84v-1.5V84H96 M97.5,82.5h-15v15h-15h-15h-15v15h15v15h15v-15h15v15v15h15v-15h15v-15h-15v-15V82.5L97.5,82.5z"
            />
          </g>
          <g>
            <path
              className="vis-graphic--fill vis-graphic--fill-c"
              d="M141,84v13.5v15V126h-12v-13.5V111h-1.5H114V99h13.5h1.5v-1.5V84H141 M142.5,82.5h-15v15h-15v15h15v15h15v-15
			v-15V82.5L142.5,82.5z"
            />
          </g>
          <g>
            <path
              className="vis-graphic--fill vis-graphic--fill-c"
              d="M36,69v13.5V96H22.5H21v1.5v15V126H9v-13.5v-15V84h13.5H24v-1.5V69H36 M37.5,67.5h-15v15h-15v15v15v15h15v-15
			v-15h15v-15V67.5L37.5,67.5z"
            />
          </g>
        </g>
        <g id="b">
          <rect
            x="67.5"
            y="82.5"
            className="vis-graphic--fill vis-graphic--fill-a"
            width="15"
            height="15"
          />
          <polygon
            className="vis-graphic--fill vis-graphic--fill-a"
            points="52.5,67.5 37.5,67.5 37.5,82.5 37.5,97.5 52.5,97.5 52.5,82.5 67.5,82.5 67.5,67.5 	"
          />
          <rect
            x="127.5"
            y="67.5"
            className="vis-graphic--fill vis-graphic--fill-a"
            width="15"
            height="15"
          />
          <polygon
            className="vis-graphic--fill vis-graphic--fill-a"
            points="22.5,52.5 7.5,52.5 7.5,67.5 22.5,67.5 37.5,67.5 37.5,52.5 	"
          />
          <rect
            x="67.5"
            y="52.5"
            className="vis-graphic--fill vis-graphic--fill-a"
            width="15"
            height="15"
          />
          <polygon
            className="vis-graphic--fill vis-graphic--fill-a"
            points="112.5,67.5 97.5,67.5 82.5,67.5 82.5,82.5 97.5,82.5 97.5,97.5 97.5,112.5 112.5,112.5 112.5,97.5
		127.5,97.5 127.5,82.5 112.5,82.5 	"
          />
          <rect
            x="82.5"
            y="37.5"
            className="vis-graphic--fill vis-graphic--fill-a"
            width="15"
            height="15"
          />
          <rect
            x="112.5"
            y="52.5"
            className="vis-graphic--fill vis-graphic--fill-a"
            width="15"
            height="15"
          />
          <rect
            x="37.5"
            y="37.5"
            className="vis-graphic--fill vis-graphic--fill-a"
            width="15"
            height="15"
          />
          <rect
            x="127.5"
            y="37.5"
            className="vis-graphic--fill vis-graphic--fill-a"
            width="15"
            height="15"
          />
          <rect
            x="22.5"
            y="22.5"
            className="vis-graphic--fill vis-graphic--fill-a"
            width="15"
            height="15"
          />
          <rect
            x="67.5"
            y="7.5"
            className="vis-graphic--fill vis-graphic--fill-a"
            width="15"
            height="15"
          />
        </g>
        <g id="bl">
          <g>
            <path
              className="vis-graphic--fill vis-graphic--fill-a"
              d="M81,84v12H69V84H81 M82.5,82.5h-15v15h15V82.5L82.5,82.5z"
            />
          </g>
          <g>
            <path
              className="vis-graphic--fill vis-graphic--fill-a"
              d="M66,69v12H52.5H51v1.5V96H39V82.5V69h13.5H66 M67.5,67.5h-15h-15v15v15h15v-15h15V67.5L67.5,67.5z"
            />
          </g>
          <g>
            <path
              className="vis-graphic--fill vis-graphic--fill-a"
              d="M141,69v12h-12V69H141 M142.5,67.5h-15v15h15V67.5L142.5,67.5z"
            />
          </g>
          <g>
            <path
              className="vis-graphic--fill vis-graphic--fill-a"
              d="M36,54v12H22.5H9V54h13.5H36 M37.5,52.5h-15h-15v15h15h15V52.5L37.5,52.5z"
            />
          </g>
          <g>
            <path
              className="vis-graphic--fill vis-graphic--fill-a"
              d="M81,54v12H69V54H81 M82.5,52.5h-15v15h15V52.5L82.5,52.5z"
            />
          </g>
          <g>
            <path
              className="vis-graphic--fill vis-graphic--fill-a"
              d="M111,69v13.5V84h1.5H126v12h-13.5H111v1.5V111H99V97.5v-15V81h-1.5H84V69h13.5H111 M112.5,67.5h-15h-15v15h15
			v15v15h15v-15h15v-15h-15V67.5L112.5,67.5z"
            />
          </g>
          <g>
            <path
              className="vis-graphic--fill vis-graphic--fill-a"
              d="M96,39v12H84V39H96 M97.5,37.5h-15v15h15V37.5L97.5,37.5z"
            />
          </g>
          <g>
            <path
              className="vis-graphic--fill vis-graphic--fill-a"
              d="M126,54v12h-12V54H126 M127.5,52.5h-15v15h15V52.5L127.5,52.5z"
            />
          </g>
          <g>
            <path
              className="vis-graphic--fill vis-graphic--fill-a"
              d="M51,39v12H39V39H51 M52.5,37.5h-15v15h15V37.5L52.5,37.5z"
            />
          </g>
          <g>
            <path
              className="vis-graphic--fill vis-graphic--fill-a"
              d="M141,39v12h-12V39H141 M142.5,37.5h-15v15h15V37.5L142.5,37.5z"
            />
          </g>
          <g>
            <path
              className="vis-graphic--fill vis-graphic--fill-a"
              d="M36,24v12H24V24H36 M37.5,22.5h-15v15h15V22.5L37.5,22.5z"
            />
          </g>
          <g>
            <path
              className="vis-graphic--fill vis-graphic--fill-a"
              d="M81,9v12H69V9H81 M82.5,7.5h-15v15h15V7.5L82.5,7.5z"
            />
          </g>
        </g>
      </svg>
    </div>
  ),
  histogram: (
    <div className="vis-graphic" data-testid="vis-graphic--histogram">
      <svg
        width="100%"
        height="100%"
        version="1.1"
        id="Histogram"
        x="0px"
        y="0px"
        viewBox="0 0 150 150"
        preserveAspectRatio="none meet"
      >
        <g>
          <rect
            x="122"
            y="51"
            className="vis-graphic--fill vis-graphic--fill-c"
            width="26"
            height="24"
          />
          <rect
            x="122"
            y="113"
            className="vis-graphic--fill vis-graphic--fill-b"
            width="26"
            height="13"
          />
          <rect
            x="122"
            y="79"
            className="vis-graphic--fill vis-graphic--fill-a"
            width="26"
            height="30"
          />
          <rect
            x="92"
            y="74"
            className="vis-graphic--fill vis-graphic--fill-b"
            width="26"
            height="52"
          />
          <rect
            x="92"
            y="49"
            className="vis-graphic--fill vis-graphic--fill-a"
            width="26"
            height="21"
          />
          <rect
            x="62"
            y="94"
            className="vis-graphic--fill vis-graphic--fill-b"
            width="26"
            height="32"
          />
          <rect
            x="62"
            y="59"
            className="vis-graphic--fill vis-graphic--fill-a"
            width="26"
            height="31"
          />
          <rect
            x="62"
            y="24"
            className="vis-graphic--fill vis-graphic--fill-c"
            width="26"
            height="31"
          />
          <rect
            x="32"
            y="116"
            className="vis-graphic--fill vis-graphic--fill-b"
            width="26"
            height="10"
          />
          <rect
            x="32"
            y="80"
            className="vis-graphic--fill vis-graphic--fill-a"
            width="26"
            height="32"
          />
          <rect
            x="2"
            y="110"
            className="vis-graphic--fill vis-graphic--fill-b"
            width="26"
            height="16"
          />
          <rect
            x="2"
            y="90"
            className="vis-graphic--fill vis-graphic--fill-a"
            width="26"
            height="16"
          />
          <rect
            x="32"
            y="64"
            className="vis-graphic--fill vis-graphic--fill-c"
            width="26"
            height="12"
          />
          <rect
            x="92"
            y="35"
            className="vis-graphic--fill vis-graphic--fill-c"
            width="26"
            height="10"
          />
          <rect
            x="2"
            y="81"
            className="vis-graphic--fill vis-graphic--fill-c"
            width="26"
            height="5"
          />
        </g>
        <g>
          <rect
            x="122"
            y="51"
            className="vis-graphic--line vis-graphic--line-c"
            width="26"
            height="24"
          />
          <rect
            x="122"
            y="113"
            className="vis-graphic--line vis-graphic--line-b"
            width="26"
            height="13"
          />
          <rect
            x="122"
            y="79"
            className="vis-graphic--line vis-graphic--line-a"
            width="26"
            height="30"
          />
          <rect
            x="92"
            y="74"
            className="vis-graphic--line vis-graphic--line-b"
            width="26"
            height="52"
          />
          <rect
            x="92"
            y="49"
            className="vis-graphic--line vis-graphic--line-a"
            width="26"
            height="21"
          />
          <rect
            x="62"
            y="94"
            className="vis-graphic--line vis-graphic--line-b"
            width="26"
            height="32"
          />
          <rect
            x="62"
            y="59"
            className="vis-graphic--line vis-graphic--line-a"
            width="26"
            height="31"
          />
          <rect
            x="62"
            y="24"
            className="vis-graphic--line vis-graphic--line-c"
            width="26"
            height="31"
          />
          <rect
            x="32"
            y="116"
            className="vis-graphic--line vis-graphic--line-b"
            width="26"
            height="10"
          />
          <rect
            x="32"
            y="80"
            className="vis-graphic--line vis-graphic--line-a"
            width="26"
            height="32"
          />
          <rect
            x="2"
            y="110"
            className="vis-graphic--line vis-graphic--line-b"
            width="26"
            height="16"
          />
          <rect
            x="2"
            y="90"
            className="vis-graphic--line vis-graphic--line-a"
            width="26"
            height="16"
          />
          <rect
            x="32"
            y="64"
            className="vis-graphic--line vis-graphic--line-c"
            width="26"
            height="12"
          />
          <rect
            x="92"
            y="35"
            className="vis-graphic--line vis-graphic--line-c"
            width="26"
            height="10"
          />
          <rect
            x="2"
            y="81"
            className="vis-graphic--line vis-graphic--line-c"
            width="26"
            height="5"
          />
        </g>
      </svg>
    </div>
  ),
  xy: (
    <div className="vis-graphic" data-testid="vis-graphic--xy">
      <svg
        width="100%"
        height="100%"
        version="1.1"
        id="Line"
        x="0px"
        y="0px"
        viewBox="0 0 150 150"
        preserveAspectRatio="none meet"
      >
        <polygon
          className="vis-graphic--fill vis-graphic--fill-a"
          points="148,40 111.5,47.2 75,25 38.5,90.8 2,111.8 2,125 148,125 	"
        />
        <polyline
          className="vis-graphic--line vis-graphic--line-a"
          points="2,111.8 38.5,90.8 75,25 111.5,47.2 148,40 	"
        />
        <polygon
          className="vis-graphic--fill vis-graphic--fill-b"
          points="148,88.2 111.5,95.5 75,61.7 38.5,49.3 2,90.8 2,125 148,125 	"
        />
        <polyline
          className="vis-graphic--line vis-graphic--line-b"
          points="2,90.8 38.5,49.3 75,61.7 111.5,95.5 148,88.2 	"
        />
        <polygon
          className="vis-graphic--fill vis-graphic--fill-c"
          points="148,96 111.5,106.3 75,85.7 38.5,116.5 2,115 2,125 148,125 	"
        />
        <polyline
          className="vis-graphic--line vis-graphic--line-c"
          points="2,115 38.5,116.5 75,85.7 111.5,106.3 148,96 	"
        />
      </svg>
    </div>
  ),
  'single-stat': (
    <div className="vis-graphic" data-testid="vis-graphic--single-stat">
      <svg
        width="100%"
        height="100%"
        version="1.1"
        id="SingleStat"
        x="0px"
        y="0px"
        viewBox="0 0 150 150"
        preserveAspectRatio="none meet"
      >
        <path
          className="vis-graphic--fill vis-graphic--fill-d"
          d="M35.6,80.4h4.9v1.1h-4.9v7.8h-1.1v-7.8H20.7v-0.6l13.6-20.1h1.3V80.4z M22.4,80.4h12.1V62.1l-1.6,2.7 L22.4,80.4z"
        />
        <path
          className="vis-graphic--fill vis-graphic--fill-d"
          d="M58.6,75.1c-0.7,1.5-1.8,2.7-3.2,3.6c-1.5,0.9-3.1,1.4-4.9,1.4c-1.6,0-3-0.4-4.2-1.3s-2.2-2-2.9-3.5 c-0.7-1.5-1.1-3.1-1.1-4.8c0-1.9,0.4-3.6,1.1-5.1c0.7-1.6,1.7-2.8,3-3.7c1.3-0.9,2.7-1.3,4.3-1.3c2.9,0,5.2,1,6.7,2.9 c1.5,1.9,2.3,4.7,2.3,8.3v3.3c0,4.8-1.1,8.5-3.2,11c-2.1,2.5-5.3,3.8-9.4,3.9H46l0-1.1h0.8c3.8,0,6.7-1.2,8.7-3.5 C57.6,82.8,58.6,79.5,58.6,75.1z M50.4,79c1.9,0,3.6-0.6,5.1-1.7s2.5-2.6,3-4.5v-1.2c0-3.3-0.7-5.8-2-7.5c-1.4-1.7-3.3-2.6-5.8-2.6 c-1.4,0-2.7,0.4-3.8,1.2s-2,1.9-2.6,3.3c-0.6,1.4-0.9,2.9-0.9,4.5c0,1.5,0.3,3,0.9,4.3c0.6,1.3,1.5,2.4,2.5,3.1 C47.8,78.7,49.1,79,50.4,79z"
        />
        <path
          className="vis-graphic--fill vis-graphic--fill-d"
          d="M81.3,89.2h-17v-1.1L74,77c1.6-1.9,2.8-3.5,3.5-5c0.8-1.4,1.2-2.8,1.2-4c0-2.1-0.6-3.7-1.8-4.9 c-1.2-1.2-2.9-1.7-5.1-1.7c-1.3,0-2.5,0.3-3.6,1c-1.1,0.6-2,1.5-2.6,2.6c-0.6,1.1-0.9,2.4-0.9,3.8h-1.1c0-1.5,0.4-2.9,1.1-4.2 c0.7-1.3,1.7-2.3,2.9-3.1s2.6-1.1,4.2-1.1c2.5,0,4.5,0.7,5.9,2c1.4,1.3,2.1,3.2,2.1,5.6c0,2.2-1.2,4.9-3.7,7.9l-1.8,2.2l-8.6,10 h15.6V89.2z"
        />
        <path
          className="vis-graphic--fill vis-graphic--fill-d"
          d="M85.3,88.3c0-0.3,0.1-0.6,0.3-0.8c0.2-0.2,0.5-0.3,0.8-0.3c0.3,0,0.6,0.1,0.8,0.3s0.3,0.5,0.3,0.8 c0,0.3-0.1,0.6-0.3,0.8s-0.5,0.3-0.8,0.3c-0.3,0-0.6-0.1-0.8-0.3C85.4,88.8,85.3,88.6,85.3,88.3z"
        />
        <path
          className="vis-graphic--fill vis-graphic--fill-d"
          d="M92.7,74.3L94,60.8h13.9v1.1H95l-1.2,11.4c0.7-0.6,1.6-1,2.7-1.4s2.2-0.5,3.3-0.5c2.6,0,4.6,0.8,6.1,2.4 c1.5,1.6,2.3,3.8,2.3,6.4c0,3.1-0.7,5.4-2.1,7c-1.4,1.6-3.4,2.4-5.9,2.4c-2.4,0-4.4-0.7-5.9-2.1c-1.5-1.4-2.3-3.3-2.5-5.8h1.1 c0.2,2.2,0.9,3.9,2.2,5.1c1.2,1.2,3,1.7,5.2,1.7c2.3,0,4.1-0.7,5.2-2.1c1.1-1.4,1.7-3.5,1.7-6.2c0-2.4-0.7-4.3-2-5.7 c-1.3-1.4-3.1-2.1-5.3-2.1c-1.4,0-2.6,0.2-3.6,0.5c-1,0.4-1.9,0.9-2.7,1.7L92.7,74.3z"
        />
        <path
          className="vis-graphic--fill vis-graphic--fill-d"
          d="M113.8,74.3l1.3-13.6H129v1.1h-12.9l-1.2,11.4c0.7-0.6,1.6-1,2.7-1.4s2.2-0.5,3.3-0.5c2.6,0,4.6,0.8,6.1,2.4 c1.5,1.6,2.3,3.8,2.3,6.4c0,3.1-0.7,5.4-2.1,7c-1.4,1.6-3.4,2.4-5.9,2.4c-2.4,0-4.4-0.7-5.9-2.1c-1.5-1.4-2.3-3.3-2.5-5.8h1.1 c0.2,2.2,0.9,3.9,2.2,5.1c1.2,1.2,3,1.7,5.2,1.7c2.3,0,4.1-0.7,5.2-2.1c1.1-1.4,1.7-3.5,1.7-6.2c0-2.4-0.7-4.3-2-5.7 c-1.3-1.4-3.1-2.1-5.3-2.1c-1.4,0-2.6,0.2-3.6,0.5c-1,0.4-1.9,0.9-2.7,1.7L113.8,74.3z"
        />
      </svg>
    </div>
  ),
  'line-plus-single-stat': (
    <div
      className="vis-graphic"
      data-testid="vis-graphic--line-plus-single-stat"
    >
      <svg
        width="100%"
        height="100%"
        version="1.1"
        id="LineAndSingleStat"
        x="0px"
        y="0px"
        viewBox="0 0 150 150"
        preserveAspectRatio="none meet"
      >
        <g>
          <polygon
            className="vis-graphic--fill vis-graphic--fill-c"
            points="148,88.2 111.5,95.5 75,25 38.5,54.7 2,66.7 2,125 148,125"
          />
          <polyline
            className="vis-graphic--line vis-graphic--line-c"
            points="2,66.7 38.5,54.7 75,25 111.5,95.5 148,88.2"
          />
        </g>
        <path
          className="vis-graphic--fill vis-graphic--fill-d"
          d="M35.6,80.4h4.9v1.1h-4.9v7.8h-1.1v-7.8H20.7v-0.6l13.6-20.1h1.3V80.4z M22.4,80.4h12.1V62.1l-1.6,2.7 L22.4,80.4z"
        />
        <path
          className="vis-graphic--fill vis-graphic--fill-d"
          d="M58.6,75.1c-0.7,1.5-1.8,2.7-3.2,3.6c-1.5,0.9-3.1,1.4-4.9,1.4c-1.6,0-3-0.4-4.2-1.3s-2.2-2-2.9-3.5 c-0.7-1.5-1.1-3.1-1.1-4.8c0-1.9,0.4-3.6,1.1-5.1c0.7-1.6,1.7-2.8,3-3.7c1.3-0.9,2.7-1.3,4.3-1.3c2.9,0,5.2,1,6.7,2.9 c1.5,1.9,2.3,4.7,2.3,8.3v3.3c0,4.8-1.1,8.5-3.2,11c-2.1,2.5-5.3,3.8-9.4,3.9H46l0-1.1h0.8c3.8,0,6.7-1.2,8.7-3.5 C57.6,82.8,58.6,79.5,58.6,75.1z M50.4,79c1.9,0,3.6-0.6,5.1-1.7s2.5-2.6,3-4.5v-1.2c0-3.3-0.7-5.8-2-7.5c-1.4-1.7-3.3-2.6-5.8-2.6 c-1.4,0-2.7,0.4-3.8,1.2s-2,1.9-2.6,3.3c-0.6,1.4-0.9,2.9-0.9,4.5c0,1.5,0.3,3,0.9,4.3c0.6,1.3,1.5,2.4,2.5,3.1 C47.8,78.7,49.1,79,50.4,79z"
        />
        <path
          className="vis-graphic--fill vis-graphic--fill-d"
          d="M81.3,89.2h-17v-1.1L74,77c1.6-1.9,2.8-3.5,3.5-5c0.8-1.4,1.2-2.8,1.2-4c0-2.1-0.6-3.7-1.8-4.9 c-1.2-1.2-2.9-1.7-5.1-1.7c-1.3,0-2.5,0.3-3.6,1c-1.1,0.6-2,1.5-2.6,2.6c-0.6,1.1-0.9,2.4-0.9,3.8h-1.1c0-1.5,0.4-2.9,1.1-4.2 c0.7-1.3,1.7-2.3,2.9-3.1s2.6-1.1,4.2-1.1c2.5,0,4.5,0.7,5.9,2c1.4,1.3,2.1,3.2,2.1,5.6c0,2.2-1.2,4.9-3.7,7.9l-1.8,2.2l-8.6,10 h15.6V89.2z"
        />
        <path
          className="vis-graphic--fill vis-graphic--fill-d"
          d="M85.3,88.3c0-0.3,0.1-0.6,0.3-0.8c0.2-0.2,0.5-0.3,0.8-0.3c0.3,0,0.6,0.1,0.8,0.3s0.3,0.5,0.3,0.8 c0,0.3-0.1,0.6-0.3,0.8s-0.5,0.3-0.8,0.3c-0.3,0-0.6-0.1-0.8-0.3C85.4,88.8,85.3,88.6,85.3,88.3z"
        />
        <path
          className="vis-graphic--fill vis-graphic--fill-d"
          d="M92.7,74.3L94,60.8h13.9v1.1H95l-1.2,11.4c0.7-0.6,1.6-1,2.7-1.4s2.2-0.5,3.3-0.5c2.6,0,4.6,0.8,6.1,2.4 c1.5,1.6,2.3,3.8,2.3,6.4c0,3.1-0.7,5.4-2.1,7c-1.4,1.6-3.4,2.4-5.9,2.4c-2.4,0-4.4-0.7-5.9-2.1c-1.5-1.4-2.3-3.3-2.5-5.8h1.1 c0.2,2.2,0.9,3.9,2.2,5.1c1.2,1.2,3,1.7,5.2,1.7c2.3,0,4.1-0.7,5.2-2.1c1.1-1.4,1.7-3.5,1.7-6.2c0-2.4-0.7-4.3-2-5.7 c-1.3-1.4-3.1-2.1-5.3-2.1c-1.4,0-2.6,0.2-3.6,0.5c-1,0.4-1.9,0.9-2.7,1.7L92.7,74.3z"
        />
        <path
          className="vis-graphic--fill vis-graphic--fill-d"
          d="M113.8,74.3l1.3-13.6H129v1.1h-12.9l-1.2,11.4c0.7-0.6,1.6-1,2.7-1.4s2.2-0.5,3.3-0.5c2.6,0,4.6,0.8,6.1,2.4 c1.5,1.6,2.3,3.8,2.3,6.4c0,3.1-0.7,5.4-2.1,7c-1.4,1.6-3.4,2.4-5.9,2.4c-2.4,0-4.4-0.7-5.9-2.1c-1.5-1.4-2.3-3.3-2.5-5.8h1.1 c0.2,2.2,0.9,3.9,2.2,5.1c1.2,1.2,3,1.7,5.2,1.7c2.3,0,4.1-0.7,5.2-2.1c1.1-1.4,1.7-3.5,1.7-6.2c0-2.4-0.7-4.3-2-5.7 c-1.3-1.4-3.1-2.1-5.3-2.1c-1.4,0-2.6,0.2-3.6,0.5c-1,0.4-1.9,0.9-2.7,1.7L113.8,74.3z"
        />
      </svg>
    </div>
  ),
  gauge: (
    <div className="vis-graphic" data-testid="vis-graphic--gauge">
      <svg
        width="100%"
        height="100%"
        version="1.1"
        id="Bar"
        x="0px"
        y="0px"
        viewBox="0 0 150 150"
        preserveAspectRatio="none meet"
      >
        <g>
          <path
            className="vis-graphic--line vis-graphic--line-d"
            d="M110.9,110.9c19.9-19.9,19.9-52,0-71.9s-52-19.9-71.9,0s-19.9,52,0,71.9"
          />
          <line
            className="vis-graphic--line vis-graphic--line-d"
            x1="39.1"
            y1="110.9"
            x2="35"
            y2="115"
          />
          <line
            className="vis-graphic--line vis-graphic--line-d"
            x1="110.9"
            y1="110.9"
            x2="115"
            y2="115"
          />
          <line
            className="vis-graphic--line vis-graphic--line-d"
            x1="122"
            y1="94.5"
            x2="127.2"
            y2="96.6"
          />
          <line
            className="vis-graphic--line vis-graphic--line-d"
            x1="125.8"
            y1="75"
            x2="131.5"
            y2="75"
          />
          <line
            className="vis-graphic--line vis-graphic--line-d"
            x1="122"
            y1="55.5"
            x2="127.2"
            y2="53.4"
          />
          <line
            className="vis-graphic--line vis-graphic--line-d"
            x1="110.9"
            y1="39.1"
            x2="115"
            y2="35"
          />
          <line
            className="vis-graphic--line vis-graphic--line-d"
            x1="94.5"
            y1="28"
            x2="96.6"
            y2="22.8"
          />
          <line
            className="vis-graphic--line vis-graphic--line-d"
            x1="75"
            y1="24.2"
            x2="75"
            y2="18.5"
          />
          <line
            className="vis-graphic--line vis-graphic--line-d"
            x1="55.5"
            y1="28"
            x2="53.4"
            y2="22.8"
          />
          <line
            className="vis-graphic--line vis-graphic--line-d"
            x1="39.1"
            y1="39.1"
            x2="35"
            y2="35"
          />
          <line
            className="vis-graphic--line vis-graphic--line-d"
            x1="28"
            y1="55.5"
            x2="22.8"
            y2="53.4"
          />
          <line
            className="vis-graphic--line vis-graphic--line-d"
            x1="24.2"
            y1="75"
            x2="18.5"
            y2="75"
          />
          <line
            className="vis-graphic--line vis-graphic--line-d"
            x1="28"
            y1="94.5"
            x2="22.8"
            y2="96.6"
          />
        </g>
        <path
          className="vis-graphic--fill vis-graphic--fill-d"
          d="M78.6,73.4L75,56.3l-3.6,17.1c-0.2,0.5-0.3,1-0.3,1.6c0,2.2,1.8,3.9,3.9,3.9s3.9-1.8,3.9-3.9C78.9,74.4,78.8,73.9,78.6,73.4z"
        />
        <path
          className="vis-graphic--fill vis-graphic--fill-a"
          d="M58.9,58.9c8.9-8.9,23.4-8.9,32.3,0l17.1-17.1c-18.4-18.4-48.2-18.4-66.5,0C32.5,50.9,27.9,63,27.9,75h24.2C52.2,69.2,54.4,63.3,58.9,58.9z"
        />
        <path
          className="vis-graphic--line vis-graphic--line-a"
          d="M58.9,58.9c8.9-8.9,23.4-8.9,32.3,0l17.1-17.1c-18.4-18.4-48.2-18.4-66.5,0C32.5,50.9,27.9,63,27.9,75h24.2C52.2,69.2,54.4,63.3,58.9,58.9z"
        />
        <path
          className="vis-graphic--fill vis-graphic--fill-b"
          d="M58.9,91.1c-4.5-4.5-6.7-10.3-6.7-16.1H27.9c0,12,4.6,24.1,13.8,33.3L58.9,91.1z"
        />
        <path
          className="vis-graphic--line vis-graphic--line-b"
          d="M58.9,91.1c-4.5-4.5-6.7-10.3-6.7-16.1H27.9c0,12,4.6,24.1,13.8,33.3L58.9,91.1z"
        />
        <path
          className="vis-graphic--fill vis-graphic--fill-c"
          d="M91.1,91.1l17.1,17.1c18.4-18.4,18.4-48.2,0-66.6L91.1,58.9C100.1,67.8,100.1,82.2,91.1,91.1z"
        />
        <path
          className="vis-graphic--line vis-graphic--line-c"
          d="M91.1,91.1l17.1,17.1c18.4-18.4,18.4-48.2,0-66.6L91.1,58.9C100.1,67.8,100.1,82.2,91.1,91.1z"
        />
      </svg>
    </div>
  ),
  table: (
    <div className="vis-graphic" data-testid="vis-graphic--table">
      <svg
        id="Table"
        x="0px"
        y="0px"
        width="100%"
        height="100%"
        viewBox="0 0 150 150"
      >
        <path
          className="vis-graphic--fill vis-graphic--fill-c"
          d="M55.5,115H19.7c-1.7,0-3.1-1.4-3.1-3.1V61.7h38.9V115z"
        />
        <path
          className="vis-graphic--fill vis-graphic--fill-b"
          d="M133.4,61.7H55.5V35h74.8c1.7,0,3.1,1.4,3.1,3.1V61.7z"
        />
        <path
          className="vis-graphic--fill vis-graphic--fill-a"
          d="M55.5,61.7H16.6V38.1c0-1.7,1.4-3.1,3.1-3.1h35.9V61.7z"
        />
        <path
          className="vis-graphic--line vis-graphic--line-c"
          d="M16.6,88.3v23.6c0,1.7,1.4,3.1,3.1,3.1h35.9V88.3H16.6z"
        />
        <rect
          className="vis-graphic--line vis-graphic--line-c"
          x="16.6"
          y="61.7"
          width="38.9"
          height="26.7"
        />
        <path
          className="vis-graphic--line vis-graphic--line-b"
          d="M94.5,35v26.7h38.9V38.1c0-1.7-1.4-3.1-3.1-3.1H94.5z"
        />
        <rect
          className="vis-graphic--line vis-graphic--line-b"
          x="55.5"
          y="35"
          width="38.9"
          height="26.7"
        />
        <path
          className="vis-graphic--line vis-graphic--line-d"
          d="M94.5,115h35.9c1.7,0,3.1-1.4,3.1-3.1V88.3H94.5V115z"
        />
        <rect
          className="vis-graphic--line vis-graphic--line-d"
          x="55.5"
          y="88.3"
          width="38.9"
          height="26.7"
        />
        <rect
          className="vis-graphic--line vis-graphic--line-d"
          x="94.5"
          y="61.7"
          width="38.9"
          height="26.7"
        />
        <rect
          className="vis-graphic--line vis-graphic--line-d"
          x="55.5"
          y="61.7"
          width="38.9"
          height="26.7"
        />
        <path
          className="vis-graphic--line vis-graphic--line-a"
          d="M55.5,35H19.7c-1.7,0-3.1,1.4-3.1,3.1v23.6h38.9V35z"
        />
      </svg>
    </div>
  ),
  scatter: (
    <div className="vis-graphic" data-testid="vis-graphic--scatter">
      <svg
        width="100%"
        height="100%"
        version="1.1"
        id="Scatter"
        x="0px"
        y="0px"
        viewBox="0 0 150 150"
        preserveAspectRatio="none meet"
      >
        <circle
          className="vis-graphic--fill vis-graphic--fill-b"
          cx="77.6"
          cy="91.1"
          r="7.5"
        />
        <circle
          className="vis-graphic--fill vis-graphic--fill-b"
          cx="47.5"
          cy="110.9"
          r="7.5"
        />
        <circle
          className="vis-graphic--fill vis-graphic--fill-b"
          cx="111.6"
          cy="46.1"
          r="7.5"
        />
        <circle
          className="vis-graphic--fill vis-graphic--fill-b"
          cx="17.5"
          cy="118.5"
          r="7.5"
        />
        <rect
          x="77.6"
          y="111"
          className="vis-graphic--fill vis-graphic--fill-c"
          width="15"
          height="15"
        />
        <rect
          x="108.3"
          y="83.6"
          className="vis-graphic--fill vis-graphic--fill-c"
          width="15"
          height="15"
        />
        <rect
          x="125"
          y="54"
          className="vis-graphic--fill vis-graphic--fill-c"
          width="15"
          height="15"
        />
        <rect
          x="123.2"
          y="111"
          className="vis-graphic--fill vis-graphic--fill-c"
          width="15"
          height="15"
        />
        <polygon
          className="vis-graphic--fill vis-graphic--fill-a"
          points="49.5,68.6 42,81.4 57,81.4 "
        />
        <polygon
          className="vis-graphic--fill vis-graphic--fill-a"
          points="61.1,25.7 53.6,38.6 68.6,38.6 "
        />
        <polygon
          className="vis-graphic--fill vis-graphic--fill-a"
          points="93.8,19.2 86.3,32.2 101.3,32.2 "
        />
        <polygon
          className="vis-graphic--fill vis-graphic--fill-a"
          points="78.8,47.5 71.3,60.5 86.3,60.5 "
        />
      </svg>
    </div>
  ),
  mosaic: (
    <div className="vis-graphic" data-testid="vis-graphic--mosaic">
      <svg
        width="100%"
        height="100%"
        version="1.1"
        id="Bar"
        x="0px"
        y="0px"
        viewBox="0 0 150 150"
        preserveAspectRatio="none meet"
      >
        <rect
          x="96"
          y="58"
          className="vis-graphic--line vis-graphic--line-b"
          width="44"
          height="34"
        />
        <rect
          x="57"
          y="95"
          className="vis-graphic--line vis-graphic--line-b"
          width="50"
          height="33"
        />
        <rect
          x="78"
          y="22"
          className="vis-graphic--line vis-graphic--line-b"
          width="26"
          height="33"
        />
        <rect
          x="96"
          y="58"
          className="vis-graphic--fill vis-graphic--fill-b"
          width="44"
          height="34"
        />
        <rect
          x="57"
          y="95"
          className="vis-graphic--fill vis-graphic--fill-b"
          width="50"
          height="33"
        />
        <rect
          x="78"
          y="22"
          className="vis-graphic--fill vis-graphic--fill-b"
          width="26"
          height="33"
        />
        <rect
          x="67"
          y="22"
          className="vis-graphic--line vis-graphic--line-c"
          width="10"
          height="33"
        />
        <rect
          x="105"
          y="22"
          className="vis-graphic--line vis-graphic--line-c"
          width="14"
          height="33"
        />
        <rect
          x="10"
          y="95"
          className="vis-graphic--line vis-graphic--line-c"
          width="23"
          height="33"
        />
        <rect
          x="24"
          y="58"
          className="vis-graphic--line vis-graphic--line-c"
          width="71"
          height="34"
        />
        <rect
          x="67"
          y="22"
          className="vis-graphic--fill vis-graphic--fill-c"
          width="10"
          height="33"
        />
        <rect
          x="105"
          y="22"
          className="vis-graphic--fill vis-graphic--fill-c"
          width="14"
          height="33"
        />
        <rect
          x="10"
          y="95"
          className="vis-graphic--fill vis-graphic--fill-c"
          width="23"
          height="33"
        />
        <rect
          x="24"
          y="58"
          className="vis-graphic--fill vis-graphic--fill-c"
          width="71"
          height="34"
        />
        <rect
          x="120"
          y="22"
          className="vis-graphic--line vis-graphic--line-a"
          width="20"
          height="33"
        />
        <rect
          x="10"
          y="22"
          className="vis-graphic--line vis-graphic--line-a"
          width="56"
          height="33"
        />
        <rect
          x="34"
          y="95"
          className="vis-graphic--line vis-graphic--line-a"
          width="22"
          height="33"
        />
        <rect
          x="108"
          y="95"
          className="vis-graphic--line vis-graphic--line-a"
          width="32"
          height="33"
        />
        <rect
          x="10"
          y="58"
          className="vis-graphic--line vis-graphic--line-a"
          width="13"
          height="34"
        />
        <rect
          x="120"
          y="22"
          className="vis-graphic--fill vis-graphic--fill-a"
          width="20"
          height="33"
        />
        <rect
          x="10"
          y="22"
          className="vis-graphic--fill vis-graphic--fill-a"
          width="56"
          height="33"
        />
        <rect
          x="34"
          y="95"
          className="vis-graphic--fill vis-graphic--fill-a"
          width="22"
          height="33"
        />
        <rect
          x="108"
          y="95"
          className="vis-graphic--fill vis-graphic--fill-a"
          width="32"
          height="33"
        />
        <rect
          x="10"
          y="58"
          className="vis-graphic--fill vis-graphic--fill-a"
          width="13"
          height="34"
        />
      </svg>
    </div>
  ),
  mosaic: (
    <div className="vis-graphic" data-testid="vis-graphic--mosaic">
      <svg
        width="100%"
        height="100%"
        version="1.1"
        id="Bar"
        x="0px"
        y="0px"
        viewBox="0 0 150 150"
        preserveAspectRatio="none meet"
      >
        <rect
          x="96"
          y="58"
          className="vis-graphic--line vis-graphic--line-b"
          width="44"
          height="34"
        />
        <rect
          x="57"
          y="95"
          className="vis-graphic--line vis-graphic--line-b"
          width="50"
          height="33"
        />
        <rect
          x="78"
          y="22"
          className="vis-graphic--line vis-graphic--line-b"
          width="26"
          height="33"
        />
        <rect
          x="96"
          y="58"
          className="vis-graphic--fill vis-graphic--fill-b"
          width="44"
          height="34"
        />
        <rect
          x="57"
          y="95"
          className="vis-graphic--fill vis-graphic--fill-b"
          width="50"
          height="33"
        />
        <rect
          x="78"
          y="22"
          className="vis-graphic--fill vis-graphic--fill-b"
          width="26"
          height="33"
        />
        <rect
          x="67"
          y="22"
          className="vis-graphic--line vis-graphic--line-c"
          width="10"
          height="33"
        />
        <rect
          x="105"
          y="22"
          className="vis-graphic--line vis-graphic--line-c"
          width="14"
          height="33"
        />
        <rect
          x="10"
          y="95"
          className="vis-graphic--line vis-graphic--line-c"
          width="23"
          height="33"
        />
        <rect
          x="24"
          y="58"
          className="vis-graphic--line vis-graphic--line-c"
          width="71"
          height="34"
        />
        <rect
          x="67"
          y="22"
          className="vis-graphic--fill vis-graphic--fill-c"
          width="10"
          height="33"
        />
        <rect
          x="105"
          y="22"
          className="vis-graphic--fill vis-graphic--fill-c"
          width="14"
          height="33"
        />
        <rect
          x="10"
          y="95"
          className="vis-graphic--fill vis-graphic--fill-c"
          width="23"
          height="33"
        />
        <rect
          x="24"
          y="58"
          className="vis-graphic--fill vis-graphic--fill-c"
          width="71"
          height="34"
        />
        <rect
          x="120"
          y="22"
          className="vis-graphic--line vis-graphic--line-a"
          width="20"
          height="33"
        />
        <rect
          x="10"
          y="22"
          className="vis-graphic--line vis-graphic--line-a"
          width="56"
          height="33"
        />
        <rect
          x="34"
          y="95"
          className="vis-graphic--line vis-graphic--line-a"
          width="22"
          height="33"
        />
        <rect
          x="108"
          y="95"
          className="vis-graphic--line vis-graphic--line-a"
          width="32"
          height="33"
        />
        <rect
          x="10"
          y="58"
          className="vis-graphic--line vis-graphic--line-a"
          width="13"
          height="34"
        />
        <rect
          x="120"
          y="22"
          className="vis-graphic--fill vis-graphic--fill-a"
          width="20"
          height="33"
        />
        <rect
          x="10"
          y="22"
          className="vis-graphic--fill vis-graphic--fill-a"
          width="56"
          height="33"
        />
        <rect
          x="34"
          y="95"
          className="vis-graphic--fill vis-graphic--fill-a"
          width="22"
          height="33"
        />
        <rect
          x="108"
          y="95"
          className="vis-graphic--fill vis-graphic--fill-a"
          width="32"
          height="33"
        />
        <rect
          x="10"
          y="58"
          className="vis-graphic--fill vis-graphic--fill-a"
          width="13"
          height="34"
        />
      </svg>
    </div>
  ),
  map: (
    <div className="vis-graphic" data-testid="vis-graphic--mosaic">
      <svg
        width="100%"
        height="100%"
        version="1.1"
        id="Bar"
        x="0px"
        y="0px"
        viewBox="0 0 150 150"
        preserveAspectRatio="none meet"
      >
        <polygon
          className="vis-graphic--fill vis-graphic--fill-a"
          points="15,100.5 42.3,114.1 69.5,100.5 42.3,86.9 "
        />
        <polygon
          className="vis-graphic--fill vis-graphic--fill-a"
          points="80.5,100.5 107.7,114.1 135,100.5 107.7,86.9 "
        />
        <polygon
          className="vis-graphic--fill vis-graphic--fill-a"
          points="47.7,84.1 75,97.8 102.3,84.1 75,70.5 "
        />
        <polygon
          className="vis-graphic--fill vis-graphic--fill-a"
          points="47.7,116.9 75,130.5 102.3,116.9 75,103.2 "
        />
        <polygon
          className="vis-graphic--line vis-graphic-line-a"
          points="15,100.5 42.3,114.1 69.5,100.5 42.3,86.9 "
        />
        <polygon
          className="vis-graphic--line vis-graphic-line-a"
          points="80.5,100.5 107.7,114.1 135,100.5 107.7,86.9 "
        />
        <polygon
          className="vis-graphic--line vis-graphic-line-a"
          points="47.7,84.1 75,97.8 102.3,84.1 75,70.5 "
        />
        <polygon
          className="vis-graphic--line vis-graphic-line-a"
          points="47.7,116.9 75,130.5 102.3,116.9 75,103.2 "
        />
        <path
          className="vis-graphic--fill vis-graphic--fill-b"
          d="M92.7,26.8c-9.8-9.8-25.6-9.8-35.4,0s-9.8,25.6,0,35.4L75,79.9l17.7-17.7C102.4,52.4,102.4,36.6,92.7,26.8z M82.8,52.3c-4.3,4.3-11.3,4.3-15.6,0c-4.3-4.3-4.3-11.3,0-15.6c4.3-4.3,11.3-4.3,15.6,0C87.1,41,87.1,48,82.8,52.3z"
        />
        <path
          className="vis-graphic--line vis-graphic--line-b"
          d="M92.7,26.8c-9.8-9.8-25.6-9.8-35.4,0s-9.8,25.6,0,35.4L75,79.9l17.7-17.7C102.4,52.4,102.4,36.6,92.7,26.8z M82.8,52.3c-4.3,4.3-11.3,4.3-15.6,0c-4.3-4.3-4.3-11.3,0-15.6c4.3-4.3,11.3-4.3,15.6,0C87.1,41,87.1,48,82.8,52.3z"
        />
      </svg>
    </div>
  ),
}

interface VisGraphic {
  type: ViewType
  name: string
  graphic: JSX.Element
}

export const VIS_GRAPHICS: VisGraphic[] = VIS_TYPES.map(
  ({type, name}): VisGraphic => {
    return {
      type,
      name,
      graphic: GRAPHIC_SVGS[type],
    }
  }
)
