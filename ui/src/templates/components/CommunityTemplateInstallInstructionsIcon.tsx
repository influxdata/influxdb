import React, {FC, CSSProperties} from 'react'

interface Props {
  strokeColor: string
  fillColor?: string
  strokeWidth?: number
  width: number
  height: number
}

export const CommunityTemplateInstallInstructionsIcon: FC<Props> = ({
  strokeColor,
  fillColor = 'none',
  strokeWidth = 2,
  width,
  height,
}) => {
  const lineStyle: CSSProperties = {
    strokeWidth,
    stroke: strokeColor,
    strokeLinecap: 'round',
    strokeLinejoin: 'round',
    strokeMiterlimit: 10,
  }

  const polygonStyle: CSSProperties = {
    ...lineStyle,
    fill: fillColor,
  }

  return (
    <svg
      width={width}
      height={height}
      version="1.1"
      id="Layer_1"
      xmlns="http://www.w3.org/2000/svg"
      x="0px"
      y="0px"
      viewBox="0 0 54 54"
      preserveAspectRatio="xMidYMid meet"
      xmlSpace="preserve"
    >
      <g>
        <g>
          <polygon
            style={polygonStyle}
            points="27,44.9 27,30.6 14.5,23.4 2,30.6 2,44.9 14.5,52 		"
          />
          <line style={lineStyle} x1="2.5" y1="30.8" x2="14.5" y2="37.7" />
          <line style={lineStyle} x1="14.5" y1="37.7" x2="14.5" y2="51.6" />
          <line style={lineStyle} x1="14.5" y1="37.7" x2="26.5" y2="30.8" />
        </g>
        <g>
          <polygon
            style={polygonStyle}
            points="52,44.9 52,30.6 39.5,23.4 27,30.6 27,44.9 39.5,52 		"
          />
          <line style={lineStyle} x1="27.5" y1="30.8" x2="39.5" y2="37.7" />
          <line style={lineStyle} x1="39.5" y1="37.7" x2="39.5" y2="51.6" />
          <line style={lineStyle} x1="39.5" y1="37.7" x2="51.5" y2="30.8" />
        </g>
        <g>
          <polygon
            style={polygonStyle}
            points="39.5,23.4 39.5,9.1 27,2 14.5,9.1 14.5,23.4 27,30.6 		"
          />
          <line style={lineStyle} x1="15" y1="9.4" x2="27" y2="16.3" />
          <line style={lineStyle} x1="27" y1="16.3" x2="27" y2="30.1" />
          <line style={lineStyle} x1="27" y1="16.3" x2="39" y2="9.4" />
        </g>
      </g>
    </svg>
  )
}
