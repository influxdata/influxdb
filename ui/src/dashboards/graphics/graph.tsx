import React from 'react'

import {ViewType} from 'src/types/v2/dashboards'

type Graphic = JSX.Element

interface GraphSVGs {
  [ViewType.Line]: Graphic
  [ViewType.Stacked]: Graphic
  [ViewType.StepPlot]: Graphic
  [ViewType.Bar]: Graphic
  [ViewType.LinePlusSingleStat]: Graphic
  [ViewType.SingleStat]: Graphic
  [ViewType.Gauge]: Graphic
  [ViewType.Table]: Graphic
}

const GRAPH_SVGS: GraphSVGs = {
  line: (
    <div className="viz-type-selector--graphic">
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
          className="viz-type-selector--graphic-fill graphic-fill-a"
          points="148,40 111.5,47.2 75,25 38.5,90.8 2,111.8 2,125 148,125 	"
        />
        <polyline
          className="viz-type-selector--graphic-line graphic-line-a"
          points="2,111.8 38.5,90.8 75,25 111.5,47.2 148,40 	"
        />
        <polygon
          className="viz-type-selector--graphic-fill graphic-fill-b"
          points="148,88.2 111.5,95.5 75,61.7 38.5,49.3 2,90.8 2,125 148,125 	"
        />
        <polyline
          className="viz-type-selector--graphic-line graphic-line-b"
          points="2,90.8 38.5,49.3 75,61.7 111.5,95.5 148,88.2 	"
        />
        <polygon
          className="viz-type-selector--graphic-fill graphic-fill-c"
          points="148,96 111.5,106.3 75,85.7 38.5,116.5 2,115 2,125 148,125 	"
        />
        <polyline
          className="viz-type-selector--graphic-line graphic-line-c"
          points="2,115 38.5,116.5 75,85.7 111.5,106.3 148,96 	"
        />
      </svg>
    </div>
  ),
  stacked: (
    <div className="viz-type-selector--graphic">
      <svg
        width="100%"
        height="100%"
        version="1.1"
        id="LineStacked"
        x="0px"
        y="0px"
        viewBox="0 0 150 150"
        preserveAspectRatio="none meet"
      >
        <polygon
          className="viz-type-selector--graphic-fill graphic-fill-a"
          points="148,25 111.5,25 75,46 38.5,39.1 2,85.5 2,125 148,125 	"
        />
        <polyline
          className="viz-type-selector--graphic-line graphic-line-a"
          points="2,85.5 38.5,39.1 75,46 111.5,25 148,25 	"
        />
        <polygon
          className="viz-type-selector--graphic-fill graphic-fill-b"
          points="148,53 111.5,49.9 75,88.5 38.5,71 2,116 2,125 148,125 	"
        />
        <polyline
          className="viz-type-selector--graphic-line graphic-line-b"
          points="2,116 38.5,71 75,88.5 111.5,49.9 148,53 	"
        />
        <polygon
          className="viz-type-selector--graphic-fill graphic-fill-c"
          points="148,86.2 111.5,88.6 75,108.6 38.5,98 2,121.1 2,125 148,125 	"
        />
        <polyline
          className="viz-type-selector--graphic-line graphic-line-c"
          points="2,121.1 38.5,98 75,108.6 111.5,88.6 148,86.2 	"
        />
      </svg>
    </div>
  ),
  'step-plot': (
    <div className="viz-type-selector--graphic">
      <svg
        width="100%"
        height="100%"
        version="1.1"
        id="StepPlot"
        x="0px"
        y="0px"
        viewBox="0 0 150 150"
        preserveAspectRatio="none meet"
      >
        <polygon
          className="viz-type-selector--graphic-fill graphic-fill-a"
          points="148,61.9 129.8,61.9 129.8,25 93.2,25 93.2,40.6 56.8,40.6 56.8,25 20.2,25 20.2,67.8 2,67.8 2,125 148,125 	"
        />
        <polyline
          className="viz-type-selector--graphic-line graphic-line-a"
          points="2,67.8 20.2,67.8 20.2,25 56.8,25 56.8,40.6 93.2,40.6 93.2,25 129.8,25 129.8,61.9 148,61.9 	"
        />
        <polygon
          className="viz-type-selector--graphic-fill graphic-fill-b"
          points="148,91.9 129.8,91.9 129.8,70.2 93.2,70.2 93.2,67 56.8,67 56.8,50.1 20.2,50.1 20.2,87 2,87 2,125 148,125 	"
        />
        <polyline
          className="viz-type-selector--graphic-line graphic-line-b"
          points="2,87 20.2,87 20.2,50.1 56.8,50.1 56.8,67 93.2,67 93.2,70.2 129.8,70.2 129.8,91.9 148,91.9 	"
        />
        <polygon
          className="viz-type-selector--graphic-fill graphic-fill-c"
          points="148,103.5 129.8,103.5 129.8,118.2 93.2,118.2 93.2,84.5 56.8,84.5 56.8,75 20.2,75 20.2,100.2 2,100.2 2,125 148,125 	"
        />
        <polyline
          className="viz-type-selector--graphic-line graphic-line-c"
          points="2,100.2 20.2,100.2 20.2,75 56.8,75 56.8,84.5 93.2,84.5 93.2,118.2 129.8,118.2 129.8,103.5 148,103.5 	"
        />
      </svg>
    </div>
  ),
  'single-stat': (
    <div className="viz-type-selector--graphic">
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
          className="viz-type-selector--graphic-fill graphic-fill-d"
          d="M35.6,80.4h4.9v1.1h-4.9v7.8h-1.1v-7.8H20.7v-0.6l13.6-20.1h1.3V80.4z M22.4,80.4h12.1V62.1l-1.6,2.7 L22.4,80.4z"
        />
        <path
          className="viz-type-selector--graphic-fill graphic-fill-d"
          d="M58.6,75.1c-0.7,1.5-1.8,2.7-3.2,3.6c-1.5,0.9-3.1,1.4-4.9,1.4c-1.6,0-3-0.4-4.2-1.3s-2.2-2-2.9-3.5 c-0.7-1.5-1.1-3.1-1.1-4.8c0-1.9,0.4-3.6,1.1-5.1c0.7-1.6,1.7-2.8,3-3.7c1.3-0.9,2.7-1.3,4.3-1.3c2.9,0,5.2,1,6.7,2.9 c1.5,1.9,2.3,4.7,2.3,8.3v3.3c0,4.8-1.1,8.5-3.2,11c-2.1,2.5-5.3,3.8-9.4,3.9H46l0-1.1h0.8c3.8,0,6.7-1.2,8.7-3.5 C57.6,82.8,58.6,79.5,58.6,75.1z M50.4,79c1.9,0,3.6-0.6,5.1-1.7s2.5-2.6,3-4.5v-1.2c0-3.3-0.7-5.8-2-7.5c-1.4-1.7-3.3-2.6-5.8-2.6 c-1.4,0-2.7,0.4-3.8,1.2s-2,1.9-2.6,3.3c-0.6,1.4-0.9,2.9-0.9,4.5c0,1.5,0.3,3,0.9,4.3c0.6,1.3,1.5,2.4,2.5,3.1 C47.8,78.7,49.1,79,50.4,79z"
        />
        <path
          className="viz-type-selector--graphic-fill graphic-fill-d"
          d="M81.3,89.2h-17v-1.1L74,77c1.6-1.9,2.8-3.5,3.5-5c0.8-1.4,1.2-2.8,1.2-4c0-2.1-0.6-3.7-1.8-4.9 c-1.2-1.2-2.9-1.7-5.1-1.7c-1.3,0-2.5,0.3-3.6,1c-1.1,0.6-2,1.5-2.6,2.6c-0.6,1.1-0.9,2.4-0.9,3.8h-1.1c0-1.5,0.4-2.9,1.1-4.2 c0.7-1.3,1.7-2.3,2.9-3.1s2.6-1.1,4.2-1.1c2.5,0,4.5,0.7,5.9,2c1.4,1.3,2.1,3.2,2.1,5.6c0,2.2-1.2,4.9-3.7,7.9l-1.8,2.2l-8.6,10 h15.6V89.2z"
        />
        <path
          className="viz-type-selector--graphic-fill graphic-fill-d"
          d="M85.3,88.3c0-0.3,0.1-0.6,0.3-0.8c0.2-0.2,0.5-0.3,0.8-0.3c0.3,0,0.6,0.1,0.8,0.3s0.3,0.5,0.3,0.8 c0,0.3-0.1,0.6-0.3,0.8s-0.5,0.3-0.8,0.3c-0.3,0-0.6-0.1-0.8-0.3C85.4,88.8,85.3,88.6,85.3,88.3z"
        />
        <path
          className="viz-type-selector--graphic-fill graphic-fill-d"
          d="M92.7,74.3L94,60.8h13.9v1.1H95l-1.2,11.4c0.7-0.6,1.6-1,2.7-1.4s2.2-0.5,3.3-0.5c2.6,0,4.6,0.8,6.1,2.4 c1.5,1.6,2.3,3.8,2.3,6.4c0,3.1-0.7,5.4-2.1,7c-1.4,1.6-3.4,2.4-5.9,2.4c-2.4,0-4.4-0.7-5.9-2.1c-1.5-1.4-2.3-3.3-2.5-5.8h1.1 c0.2,2.2,0.9,3.9,2.2,5.1c1.2,1.2,3,1.7,5.2,1.7c2.3,0,4.1-0.7,5.2-2.1c1.1-1.4,1.7-3.5,1.7-6.2c0-2.4-0.7-4.3-2-5.7 c-1.3-1.4-3.1-2.1-5.3-2.1c-1.4,0-2.6,0.2-3.6,0.5c-1,0.4-1.9,0.9-2.7,1.7L92.7,74.3z"
        />
        <path
          className="viz-type-selector--graphic-fill graphic-fill-d"
          d="M113.8,74.3l1.3-13.6H129v1.1h-12.9l-1.2,11.4c0.7-0.6,1.6-1,2.7-1.4s2.2-0.5,3.3-0.5c2.6,0,4.6,0.8,6.1,2.4 c1.5,1.6,2.3,3.8,2.3,6.4c0,3.1-0.7,5.4-2.1,7c-1.4,1.6-3.4,2.4-5.9,2.4c-2.4,0-4.4-0.7-5.9-2.1c-1.5-1.4-2.3-3.3-2.5-5.8h1.1 c0.2,2.2,0.9,3.9,2.2,5.1c1.2,1.2,3,1.7,5.2,1.7c2.3,0,4.1-0.7,5.2-2.1c1.1-1.4,1.7-3.5,1.7-6.2c0-2.4-0.7-4.3-2-5.7 c-1.3-1.4-3.1-2.1-5.3-2.1c-1.4,0-2.6,0.2-3.6,0.5c-1,0.4-1.9,0.9-2.7,1.7L113.8,74.3z"
        />
      </svg>
    </div>
  ),
  'line-plus-single-stat': (
    <div className="viz-type-selector--graphic">
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
            className="viz-type-selector--graphic-fill graphic-fill-c"
            points="148,88.2 111.5,95.5 75,25 38.5,54.7 2,66.7 2,125 148,125"
          />
          <polyline
            className="viz-type-selector--graphic-line graphic-line-c"
            points="2,66.7 38.5,54.7 75,25 111.5,95.5 148,88.2"
          />
        </g>
        <path
          className="viz-type-selector--graphic-fill graphic-fill-d"
          d="M35.6,80.4h4.9v1.1h-4.9v7.8h-1.1v-7.8H20.7v-0.6l13.6-20.1h1.3V80.4z M22.4,80.4h12.1V62.1l-1.6,2.7 L22.4,80.4z"
        />
        <path
          className="viz-type-selector--graphic-fill graphic-fill-d"
          d="M58.6,75.1c-0.7,1.5-1.8,2.7-3.2,3.6c-1.5,0.9-3.1,1.4-4.9,1.4c-1.6,0-3-0.4-4.2-1.3s-2.2-2-2.9-3.5 c-0.7-1.5-1.1-3.1-1.1-4.8c0-1.9,0.4-3.6,1.1-5.1c0.7-1.6,1.7-2.8,3-3.7c1.3-0.9,2.7-1.3,4.3-1.3c2.9,0,5.2,1,6.7,2.9 c1.5,1.9,2.3,4.7,2.3,8.3v3.3c0,4.8-1.1,8.5-3.2,11c-2.1,2.5-5.3,3.8-9.4,3.9H46l0-1.1h0.8c3.8,0,6.7-1.2,8.7-3.5 C57.6,82.8,58.6,79.5,58.6,75.1z M50.4,79c1.9,0,3.6-0.6,5.1-1.7s2.5-2.6,3-4.5v-1.2c0-3.3-0.7-5.8-2-7.5c-1.4-1.7-3.3-2.6-5.8-2.6 c-1.4,0-2.7,0.4-3.8,1.2s-2,1.9-2.6,3.3c-0.6,1.4-0.9,2.9-0.9,4.5c0,1.5,0.3,3,0.9,4.3c0.6,1.3,1.5,2.4,2.5,3.1 C47.8,78.7,49.1,79,50.4,79z"
        />
        <path
          className="viz-type-selector--graphic-fill graphic-fill-d"
          d="M81.3,89.2h-17v-1.1L74,77c1.6-1.9,2.8-3.5,3.5-5c0.8-1.4,1.2-2.8,1.2-4c0-2.1-0.6-3.7-1.8-4.9 c-1.2-1.2-2.9-1.7-5.1-1.7c-1.3,0-2.5,0.3-3.6,1c-1.1,0.6-2,1.5-2.6,2.6c-0.6,1.1-0.9,2.4-0.9,3.8h-1.1c0-1.5,0.4-2.9,1.1-4.2 c0.7-1.3,1.7-2.3,2.9-3.1s2.6-1.1,4.2-1.1c2.5,0,4.5,0.7,5.9,2c1.4,1.3,2.1,3.2,2.1,5.6c0,2.2-1.2,4.9-3.7,7.9l-1.8,2.2l-8.6,10 h15.6V89.2z"
        />
        <path
          className="viz-type-selector--graphic-fill graphic-fill-d"
          d="M85.3,88.3c0-0.3,0.1-0.6,0.3-0.8c0.2-0.2,0.5-0.3,0.8-0.3c0.3,0,0.6,0.1,0.8,0.3s0.3,0.5,0.3,0.8 c0,0.3-0.1,0.6-0.3,0.8s-0.5,0.3-0.8,0.3c-0.3,0-0.6-0.1-0.8-0.3C85.4,88.8,85.3,88.6,85.3,88.3z"
        />
        <path
          className="viz-type-selector--graphic-fill graphic-fill-d"
          d="M92.7,74.3L94,60.8h13.9v1.1H95l-1.2,11.4c0.7-0.6,1.6-1,2.7-1.4s2.2-0.5,3.3-0.5c2.6,0,4.6,0.8,6.1,2.4 c1.5,1.6,2.3,3.8,2.3,6.4c0,3.1-0.7,5.4-2.1,7c-1.4,1.6-3.4,2.4-5.9,2.4c-2.4,0-4.4-0.7-5.9-2.1c-1.5-1.4-2.3-3.3-2.5-5.8h1.1 c0.2,2.2,0.9,3.9,2.2,5.1c1.2,1.2,3,1.7,5.2,1.7c2.3,0,4.1-0.7,5.2-2.1c1.1-1.4,1.7-3.5,1.7-6.2c0-2.4-0.7-4.3-2-5.7 c-1.3-1.4-3.1-2.1-5.3-2.1c-1.4,0-2.6,0.2-3.6,0.5c-1,0.4-1.9,0.9-2.7,1.7L92.7,74.3z"
        />
        <path
          className="viz-type-selector--graphic-fill graphic-fill-d"
          d="M113.8,74.3l1.3-13.6H129v1.1h-12.9l-1.2,11.4c0.7-0.6,1.6-1,2.7-1.4s2.2-0.5,3.3-0.5c2.6,0,4.6,0.8,6.1,2.4 c1.5,1.6,2.3,3.8,2.3,6.4c0,3.1-0.7,5.4-2.1,7c-1.4,1.6-3.4,2.4-5.9,2.4c-2.4,0-4.4-0.7-5.9-2.1c-1.5-1.4-2.3-3.3-2.5-5.8h1.1 c0.2,2.2,0.9,3.9,2.2,5.1c1.2,1.2,3,1.7,5.2,1.7c2.3,0,4.1-0.7,5.2-2.1c1.1-1.4,1.7-3.5,1.7-6.2c0-2.4-0.7-4.3-2-5.7 c-1.3-1.4-3.1-2.1-5.3-2.1c-1.4,0-2.6,0.2-3.6,0.5c-1,0.4-1.9,0.9-2.7,1.7L113.8,74.3z"
        />
      </svg>
    </div>
  ),
  bar: (
    <div className="viz-type-selector--graphic">
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
          x="2"
          y="108.4"
          className="viz-type-selector--graphic-line graphic-line-a"
          width="26.8"
          height="16.6"
        />
        <rect
          x="31.8"
          y="82.4"
          className="viz-type-selector--graphic-line graphic-line-b"
          width="26.8"
          height="42.6"
        />
        <rect
          x="61.6"
          y="28.8"
          className="viz-type-selector--graphic-line graphic-line-c"
          width="26.8"
          height="96.2"
        />
        <rect
          x="91.4"
          y="47.9"
          className="viz-type-selector--graphic-line graphic-line-a"
          width="26.8"
          height="77.1"
        />
        <rect
          x="121.2"
          y="25"
          className="viz-type-selector--graphic-line graphic-line-b"
          width="26.8"
          height="100"
        />
        <rect
          x="2"
          y="108.4"
          className="viz-type-selector--graphic-fill graphic-fill-a"
          width="26.8"
          height="16.6"
        />
        <rect
          x="31.8"
          y="82.4"
          className="viz-type-selector--graphic-fill graphic-fill-b"
          width="26.8"
          height="42.6"
        />
        <rect
          x="61.6"
          y="28.8"
          className="viz-type-selector--graphic-fill graphic-fill-c"
          width="26.8"
          height="96.2"
        />
        <rect
          x="91.4"
          y="47.9"
          className="viz-type-selector--graphic-fill graphic-fill-a"
          width="26.8"
          height="77.1"
        />
        <rect
          x="121.2"
          y="25"
          className="viz-type-selector--graphic-fill graphic-fill-b"
          width="26.8"
          height="100"
        />
      </svg>
    </div>
  ),
  gauge: (
    <div className="viz-type-selector--graphic">
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
            className="viz-type-selector--graphic-line graphic-line-d"
            d="M110.9,110.9c19.9-19.9,19.9-52,0-71.9s-52-19.9-71.9,0s-19.9,52,0,71.9"
          />
          <line
            className="viz-type-selector--graphic-line graphic-line-d"
            x1="39.1"
            y1="110.9"
            x2="35"
            y2="115"
          />
          <line
            className="viz-type-selector--graphic-line graphic-line-d"
            x1="110.9"
            y1="110.9"
            x2="115"
            y2="115"
          />
          <line
            className="viz-type-selector--graphic-line graphic-line-d"
            x1="122"
            y1="94.5"
            x2="127.2"
            y2="96.6"
          />
          <line
            className="viz-type-selector--graphic-line graphic-line-d"
            x1="125.8"
            y1="75"
            x2="131.5"
            y2="75"
          />
          <line
            className="viz-type-selector--graphic-line graphic-line-d"
            x1="122"
            y1="55.5"
            x2="127.2"
            y2="53.4"
          />
          <line
            className="viz-type-selector--graphic-line graphic-line-d"
            x1="110.9"
            y1="39.1"
            x2="115"
            y2="35"
          />
          <line
            className="viz-type-selector--graphic-line graphic-line-d"
            x1="94.5"
            y1="28"
            x2="96.6"
            y2="22.8"
          />
          <line
            className="viz-type-selector--graphic-line graphic-line-d"
            x1="75"
            y1="24.2"
            x2="75"
            y2="18.5"
          />
          <line
            className="viz-type-selector--graphic-line graphic-line-d"
            x1="55.5"
            y1="28"
            x2="53.4"
            y2="22.8"
          />
          <line
            className="viz-type-selector--graphic-line graphic-line-d"
            x1="39.1"
            y1="39.1"
            x2="35"
            y2="35"
          />
          <line
            className="viz-type-selector--graphic-line graphic-line-d"
            x1="28"
            y1="55.5"
            x2="22.8"
            y2="53.4"
          />
          <line
            className="viz-type-selector--graphic-line graphic-line-d"
            x1="24.2"
            y1="75"
            x2="18.5"
            y2="75"
          />
          <line
            className="viz-type-selector--graphic-line graphic-line-d"
            x1="28"
            y1="94.5"
            x2="22.8"
            y2="96.6"
          />
        </g>
        <path
          className="viz-type-selector--graphic-fill graphic-fill-d"
          d="M78.6,73.4L75,56.3l-3.6,17.1c-0.2,0.5-0.3,1-0.3,1.6c0,2.2,1.8,3.9,3.9,3.9s3.9-1.8,3.9-3.9C78.9,74.4,78.8,73.9,78.6,73.4z"
        />
        <path
          className="viz-type-selector--graphic-fill graphic-fill-a"
          d="M58.9,58.9c8.9-8.9,23.4-8.9,32.3,0l17.1-17.1c-18.4-18.4-48.2-18.4-66.5,0C32.5,50.9,27.9,63,27.9,75h24.2C52.2,69.2,54.4,63.3,58.9,58.9z"
        />
        <path
          className="viz-type-selector--graphic-line graphic-line-a"
          d="M58.9,58.9c8.9-8.9,23.4-8.9,32.3,0l17.1-17.1c-18.4-18.4-48.2-18.4-66.5,0C32.5,50.9,27.9,63,27.9,75h24.2C52.2,69.2,54.4,63.3,58.9,58.9z"
        />
        <path
          className="viz-type-selector--graphic-fill graphic-fill-b"
          d="M58.9,91.1c-4.5-4.5-6.7-10.3-6.7-16.1H27.9c0,12,4.6,24.1,13.8,33.3L58.9,91.1z"
        />
        <path
          className="viz-type-selector--graphic-line graphic-line-b"
          d="M58.9,91.1c-4.5-4.5-6.7-10.3-6.7-16.1H27.9c0,12,4.6,24.1,13.8,33.3L58.9,91.1z"
        />
        <path
          className="viz-type-selector--graphic-fill graphic-fill-c"
          d="M91.1,91.1l17.1,17.1c18.4-18.4,18.4-48.2,0-66.6L91.1,58.9C100.1,67.8,100.1,82.2,91.1,91.1z"
        />
        <path
          className="viz-type-selector--graphic-line graphic-line-c"
          d="M91.1,91.1l17.1,17.1c18.4-18.4,18.4-48.2,0-66.6L91.1,58.9C100.1,67.8,100.1,82.2,91.1,91.1z"
        />
      </svg>
    </div>
  ),
  table: (
    <div className="viz-type-selector--graphic">
      <svg
        id="Table"
        x="0px"
        y="0px"
        width="100%"
        height="100%"
        viewBox="0 0 150 150"
      >
        <path
          className="viz-type-selector--graphic-fill graphic-fill-c"
          d="M55.5,115H19.7c-1.7,0-3.1-1.4-3.1-3.1V61.7h38.9V115z"
        />
        <path
          className="viz-type-selector--graphic-fill graphic-fill-b"
          d="M133.4,61.7H55.5V35h74.8c1.7,0,3.1,1.4,3.1,3.1V61.7z"
        />
        <path
          className="viz-type-selector--graphic-fill graphic-fill-a"
          d="M55.5,61.7H16.6V38.1c0-1.7,1.4-3.1,3.1-3.1h35.9V61.7z"
        />
        <path
          className="viz-type-selector--graphic-line graphic-line-c"
          d="M16.6,88.3v23.6c0,1.7,1.4,3.1,3.1,3.1h35.9V88.3H16.6z"
        />
        <rect
          className="viz-type-selector--graphic-line graphic-line-c"
          x="16.6"
          y="61.7"
          width="38.9"
          height="26.7"
        />
        <path
          className="viz-type-selector--graphic-line graphic-line-b"
          d="M94.5,35v26.7h38.9V38.1c0-1.7-1.4-3.1-3.1-3.1H94.5z"
        />
        <rect
          className="viz-type-selector--graphic-line graphic-line-b"
          x="55.5"
          y="35"
          width="38.9"
          height="26.7"
        />
        <path
          className="viz-type-selector--graphic-line graphic-line-d"
          d="M94.5,115h35.9c1.7,0,3.1-1.4,3.1-3.1V88.3H94.5V115z"
        />
        <rect
          className="viz-type-selector--graphic-line graphic-line-d"
          x="55.5"
          y="88.3"
          width="38.9"
          height="26.7"
        />
        <rect
          className="viz-type-selector--graphic-line graphic-line-d"
          x="94.5"
          y="61.7"
          width="38.9"
          height="26.7"
        />
        <rect
          className="viz-type-selector--graphic-line graphic-line-d"
          x="55.5"
          y="61.7"
          width="38.9"
          height="26.7"
        />
        <path
          className="viz-type-selector--graphic-line graphic-line-a"
          d="M55.5,35H19.7c-1.7,0-3.1,1.4-3.1,3.1v23.6h38.9V35z"
        />
      </svg>
    </div>
  ),
}

interface GraphType {
  type: ViewType
  menuOption: string
  graphic: Graphic
}
export const GRAPH_TYPES: GraphType[] = [
  {
    type: ViewType.Line,
    menuOption: 'Line Graph',
    graphic: GRAPH_SVGS[ViewType.Line],
  },
  {
    type: ViewType.Stacked,
    menuOption: 'Stacked Graph',
    graphic: GRAPH_SVGS[ViewType.Stacked],
  },
  {
    type: ViewType.StepPlot,
    menuOption: 'Step-Plot Graph',
    graphic: GRAPH_SVGS[ViewType.StepPlot],
  },
  {
    type: ViewType.Bar,
    menuOption: 'Bar Graph',
    graphic: GRAPH_SVGS[ViewType.Bar],
  },
  {
    type: ViewType.LinePlusSingleStat,
    menuOption: 'Line Graph + Single Stat',
    graphic: GRAPH_SVGS[ViewType.LinePlusSingleStat],
  },
  {
    type: ViewType.SingleStat,
    menuOption: 'Single Stat',
    graphic: GRAPH_SVGS[ViewType.SingleStat],
  },
  {
    type: ViewType.Gauge,
    menuOption: 'Gauge',
    graphic: GRAPH_SVGS[ViewType.Gauge],
  },
  {
    type: ViewType.Table,
    menuOption: 'Table',
    graphic: GRAPH_SVGS[ViewType.Table],
  },
]
