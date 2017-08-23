import React from 'react'

export const graphTypes = [
  {
    type: 'line',
    menuOption: 'Line',
    graphic: (
      <div className="viz-type-selector--graphic">
        <svg
          width="100%"
          height="100%"
          version="1.1"
          id="Line"
          x="0px"
          y="0px"
          viewBox="0 0 300 150"
          preserveAspectRatio="none meet"
        >
          <polyline
            className="viz-type-selector--graphic-line graphic-line-a"
            points="5,122.2 63,81.2 121,95.5 179,40.2 237,108.5 295,83.2"
          />
          <polygon
            className="viz-type-selector--graphic-fill graphic-fill-a"
            points="5,122.2 5,145 295,145 295,83.2 237,108.5 179,40.2 121,95.5 63,81.2"
          />
          <polyline
            className="viz-type-selector--graphic-line graphic-line-b"
            points="5,88.5 63,95 121,36.2 179,19 237,126.2 295,100.8"
          />
          <polygon
            className="viz-type-selector--graphic-fill graphic-fill-b"
            points="5,88.5 5,145 295,145 295,100.8 237,126.2 179,19 121,36.2 63,95"
          />
          <polyline
            className="viz-type-selector--graphic-line graphic-line-c"
            points="5,76.2 63,90.2 121,59.2 179,31.5 237,79.8 295,93.5"
          />
          <polygon
            className="viz-type-selector--graphic-fill graphic-fill-c"
            points="5,76.2 5,145 295,145 295,93.5 237,79.8 179,31.5 121,59.2 63,90.2"
          />
        </svg>
      </div>
    ),
  },
  {
    type: 'line-stacked',
    menuOption: 'Stacked',
    graphic: (
      <div className="viz-type-selector--graphic">
        <svg
          width="100%"
          height="100%"
          version="1.1"
          id="LineStacked"
          x="0px"
          y="0px"
          viewBox="0 0 300 150"
          preserveAspectRatio="none meet"
        >
          <polyline
            className="viz-type-selector--graphic-line graphic-line-a"
            points="5,97.5 63,111.8 121,36.2 179,51 237,102.9 295,70.2"
          />
          <polyline
            className="viz-type-selector--graphic-line graphic-line-b"
            points="5,58.8 63,81.2 121,5 179,40.2 237,96.2 295,49"
          />
          <polyline
            className="viz-type-selector--graphic-line graphic-line-c"
            points="5,107.5 63,128.5 121,79.8 179,76.5 237,113.2 295,93.5"
          />
          <polygon
            className="viz-type-selector--graphic-fill graphic-fill-a"
            points="179,51 121,36.2 63,111.8 5,97.5 5,107.5 63,128.5 121,79.8 179,76.5 237,113.2 295,93.5 295,70.2 237,102.9"
          />
          <polygon
            className="viz-type-selector--graphic-fill graphic-fill-b"
            points="237,96.2 179,40.2 121,5 63,81.2 5,58.8 5,97.5 63,111.8 121,36.2 179,51 237,102.9 295,70.2 295,49"
          />
          <polygon
            className="viz-type-selector--graphic-fill graphic-fill-c"
            points="179,76.5 121,79.8 63,128.5 5,107.5 5,145 295,145 295,93.5 237,113.2"
          />
        </svg>
      </div>
    ),
  },
  {
    type: 'line-stepplot',
    menuOption: 'Step-Plot',
    graphic: (
      <div className="viz-type-selector--graphic">
        <svg
          width="100%"
          height="100%"
          version="1.1"
          id="StepPlot"
          x="0px"
          y="0px"
          viewBox="0 0 300 150"
          preserveAspectRatio="none meet"
        >
          <polygon
            className="viz-type-selector--graphic-fill graphic-fill-a"
            points="295,85.5 266,85.5 266,108.5 208,108.5 208,94.5 150,94.5 150,41 92,41 92,66.6 34,66.6 34,54.8 5,54.8 5,145 295,145"
          />
          <polyline
            className="viz-type-selector--graphic-line graphic-line-a"
            points="5,54.8 34,54.8 34,66.6 92,66.6 92,41 150,41 150,94.5 208,94.5 208,108.5 266,108.5 266,85.5 295,85.5"
          />
          <polygon
            className="viz-type-selector--graphic-fill graphic-fill-b"
            points="34,111 34,85.8 92,85.8 92,5 150,5 150,24.5 208,24.5 208,128.2 266,128.2 266,75 295,75 295,145 5,145 5,111"
          />
          <polyline
            className="viz-type-selector--graphic-line graphic-line-b"
            points="5,111 34,111 34,85.8 92,85.8 92,5 150,5 150,24.5 208,24.5 208,128.2 266,128.2 266,75 295,75"
          />
        </svg>
      </div>
    ),
  },
  {
    type: 'single-stat',
    menuOption: 'SingleStat',
    graphic: (
      <div className="viz-type-selector--graphic">
        <svg
          width="100%"
          height="100%"
          version="1.1"
          id="SingleStat"
          x="0px"
          y="0px"
          viewBox="0 0 300 150"
          preserveAspectRatio="none meet"
        >
          <path
            className="viz-type-selector--graphic-line graphic-line-a"
            d="M243.3,39.6h-37.9v32.7c0-6.3,5.1-11.4,11.4-11.4h15.2c6.3,0,11.4,5.1,11.4,11.4v26.8c0,6.3-5.1,11.4-11.4,11.4 h-15.2c-6.3,0-11.4-5.1-11.4-11.4V88.6"
          />
          <polyline
            className="viz-type-selector--graphic-line graphic-line-a"
            points="94.6,89.1 56.7,89.1 83.2,39.6 83.2,110.4 "
          />
          <path
            className="viz-type-selector--graphic-line graphic-line-a"
            d="M144.2,77.8c0,6.3-5.1,11.4-11.4,11.4h-15.2c-6.3,0-11.4-5.1-11.4-11.4V50.9c0-6.3,5.1-11.4,11.4-11.4h15.2 c6.3,0,11.4,5.1,11.4,11.4v48.1c0,6.3-5.1,11.4-11.4,11.4h-15.2c-6.3,0-11.4-5.1-11.4-11.4"
          />
          <path
            className="viz-type-selector--graphic-line graphic-line-a"
            d="M155.8,50.9c0-6.3,5.1-11.4,11.4-11.4h15.2c6.3,0,11.4,5.1,11.4,11.4c0,24.1-37.9,24.8-37.9,59.5h37.9"
          />
        </svg>
      </div>
    ),
  },
  {
    type: 'line-plus-single-stat',
    menuOption: 'Line + Stat',
    graphic: (
      <div className="viz-type-selector--graphic">
        <svg
          width="100%"
          height="100%"
          version="1.1"
          id="LineAndSingleStat"
          x="0px"
          y="0px"
          viewBox="0 0 300 150"
          preserveAspectRatio="none meet"
        >
          <polygon
            className="viz-type-selector--graphic-fill graphic-fill-b"
            points="5,122.2 5,145 295,145 295,38.3 237,41.3 179,50 121,126.3 63,90.7"
          />
          <polyline
            className="viz-type-selector--graphic-line graphic-line-b"
            points="5,122.2 63,90.7 121,126.3 179,50 237,41.3 295,38.3"
          />
          <polygon
            className="viz-type-selector--graphic-fill graphic-fill-c"
            points="5,26.2 5,145 295,145 295,132.3 239.3,113.3 179,15 121,25 63,71.7"
          />
          <polyline
            className="viz-type-selector--graphic-line graphic-line-c"
            points="5,26.2 63,71.7 121,25 179,15 239.3,113.3 295,132.3"
          />
          <path
            className="viz-type-selector--graphic-line graphic-line-a"
            d="M243.3,39.6h-37.9v32.7c0-6.3,5.1-11.4,11.4-11.4h15.2c6.3,0,11.4,5.1,11.4,11.4v26.8 c0,6.3-5.1,11.4-11.4,11.4h-15.2c-6.3,0-11.4-5.1-11.4-11.4V88.6"
          />
          <polyline
            className="viz-type-selector--graphic-line graphic-line-a"
            points="94.6,89.1 56.7,89.1 83.2,39.6 83.2,110.4"
          />
          <path
            className="viz-type-selector--graphic-line graphic-line-a"
            d="M144.2,77.8c0,6.3-5.1,11.4-11.4,11.4h-15.2c-6.3,0-11.4-5.1-11.4-11.4V50.9c0-6.3,5.1-11.4,11.4-11.4h15.2 c6.3,0,11.4,5.1,11.4,11.4v48.1c0,6.3-5.1,11.4-11.4,11.4h-15.2c-6.3,0-11.4-5.1-11.4-11.4"
          />
          <path
            className="viz-type-selector--graphic-line graphic-line-a"
            d="M155.8,50.9c0-6.3,5.1-11.4,11.4-11.4h15.2c6.3,0,11.4,5.1,11.4,11.4c0,24.1-37.9,24.8-37.9,59.5h37.9"
          />
        </svg>
      </div>
    ),
  },
  {
    type: 'bar',
    menuOption: 'Bar',
    graphic: (
      <div className="viz-type-selector--graphic">
        <svg
          width="100%"
          height="100%"
          version="1.1"
          id="Bar"
          x="0px"
          y="0px"
          viewBox="0 0 300 150"
          preserveAspectRatio="none meet"
        >
          <path
            className="viz-type-selector--graphic-fill graphic-fill-a"
            d="M145,7c0-1.1-0.9-2-2-2h-36c-1.1,0-2,0.9-2,2v136c0,1.1,0.9,2,2,2h36c1.1,0,2-0.9,2-2V7z"
          />
          <path
            className="viz-type-selector--graphic-fill graphic-fill-c"
            d="M195,57c0-1.1-0.9-2-2-2h-36c-1.1,0-2,0.9-2,2v86c0,1.1,0.9,2,2,2h36c1.1,0,2-0.9,2-2V57z"
          />
          <path
            className="viz-type-selector--graphic-fill graphic-fill-b"
            d="M245,117c0-1.1-0.9-2-2-2h-36c-1.1,0-2,0.9-2,2v26c0,1.1,0.9,2,2,2h36c1.1,0,2-0.9,2-2V117z"
          />
          <path
            className="viz-type-selector--graphic-fill graphic-fill-a"
            d="M295,107c0-1.1-0.9-2-2-2h-36c-1.1,0-2,0.9-2,2v36c0,1.1,0.9,2,2,2h36c1.1,0,2-0.9,2-2V107z"
          />
          <path
            className="viz-type-selector--graphic-fill graphic-fill-b"
            d="M95,87c0-1.1-0.9-2-2-2H57c-1.1,0-2,0.9-2,2v56c0,1.1,0.9,2,2,2h36c1.1,0,2-0.9,2-2V87z"
          />
          <path
            className="viz-type-selector--graphic-fill graphic-fill-c"
            d="M45,130c0-1.1-0.9-2-2-2H7c-1.1,0-2,0.9-2,2v13c0,1.1,0.9,2,2,2h36c1.1,0,2-0.9,2-2V130z"
          />
          <path
            className="viz-type-selector--graphic-line graphic-line-a"
            d="M145,7c0-1.1-0.9-2-2-2h-36c-1.1,0-2,0.9-2,2v136c0,1.1,0.9,2,2,2h36c1.1,0,2-0.9,2-2V7z"
          />
          <path
            className="viz-type-selector--graphic-line graphic-line-c"
            d="M195,57c0-1.1-0.9-2-2-2h-36c-1.1,0-2,0.9-2,2v86c0,1.1,0.9,2,2,2h36c1.1,0,2-0.9,2-2V57z"
          />
          <path
            className="viz-type-selector--graphic-line graphic-line-b"
            d="M245,117c0-1.1-0.9-2-2-2h-36c-1.1,0-2,0.9-2,2v26c0,1.1,0.9,2,2,2h36c1.1,0,2-0.9,2-2V117z"
          />
          <path
            className="viz-type-selector--graphic-line graphic-line-a"
            d="M295,107c0-1.1-0.9-2-2-2h-36c-1.1,0-2,0.9-2,2v36c0,1.1,0.9,2,2,2h36c1.1,0,2-0.9,2-2V107z"
          />
          <path
            className="viz-type-selector--graphic-line graphic-line-b"
            d="M95,87c0-1.1-0.9-2-2-2H57c-1.1,0-2,0.9-2,2v56c0,1.1,0.9,2,2,2h36c1.1,0,2-0.9,2-2V87z"
          />
          <path
            className="viz-type-selector--graphic-line graphic-line-c"
            d="M45,130c0-1.1-0.9-2-2-2H7c-1.1,0-2,0.9-2,2v13c0,1.1,0.9,2,2,2h36c1.1,0,2-0.9,2-2V130z"
          />
        </svg>
      </div>
    ),
  },
]
