// Libraries
import React, {FunctionComponent} from 'react'
import classnames from 'classnames'

interface Props {
  animate: boolean
}

const CollectorGraphic: FunctionComponent<Props> = ({animate}) => {
  const className = classnames('getting-started--image collector-graphic', {
    'getting-started--image__animating': animate,
  })
  return (
    <div className={className}>
      <svg
        version="1.1"
        x="0px"
        y="0px"
        width="322px"
        height="225px"
        viewBox="0 0 322 225"
      >
        <g id="Bucket">
          <path
            id="BG"
            className="collector-graphic--bg"
            d="M161,199c-14.1,0-29.2-3.8-29.8-14.4l-6.9-51.3v-0.5c0-13.1,23-15.1,36.7-15.1s36.7,2,36.7,15.1v0.5
    l-6.9,51.3C190.2,195.2,175.1,199,161,199z"
          />
          <path
            id="BucketExterior"
            className="collector-graphic--bucket"
            d="M132.3,132.8c0-3.9,12.8-7.1,28.7-7.1s28.7,3.2,28.7,7.1l-6.9,51.1
    c0,3.9-9.7,7.1-21.8,7.1c-12.1,0-21.8-3.1-21.8-7.1L132.3,132.8z"
          />
          <ellipse
            id="BucketHole"
            className="collector-graphic--bucket-hole"
            cx="160.8"
            cy="132.8"
            rx="25.1"
            ry="4.7"
          />
        </g>
        <g id="Blocks">
          <path
            className="collector-graphic--bg"
            d="M178.2,66.5c0,1.1-0.9,2-2,2h-30.5c-1.1,0-2-0.9-2-2V36c0-1.1,0.9-2,2-2h30.5c1.1,0,2,0.9,2,2V66.5z"
          />
          <path
            className="collector-graphic--bg"
            d="M121.4,66.5c0,1.1-0.9,2-2,2H88.9c-1.1,0-2-0.9-2-2V36c0-1.1,0.9-2,2-2h30.5c1.1,0,2,0.9,2,2V66.5z"
          />
          <path
            className="collector-graphic--bg"
            d="M64.5,66.5c0,1.1-0.9,2-2,2H32c-1.1,0-2-0.9-2-2V36c0-1.1,0.9-2,2-2h30.5c1.1,0,2,0.9,2,2V66.5z"
          />
          <path
            className="collector-graphic--bg"
            d="M235.1,66.5c0,1.1-0.9,2-2,2h-30.5c-1.1,0-2-0.9-2-2V36c0-1.1,0.9-2,2-2h30.5c1.1,0,2,0.9,2,2V66.5z"
          />
          <path
            className="collector-graphic--bg"
            d="M292,66.5c0,1.1-0.9,2-2,2h-30.5c-1.1,0-2-0.9-2-2V36c0-1.1,0.9-2,2-2H290c1.1,0,2,0.9,2,2V66.5z"
          />
        </g>
        <g id="Lines">
          <path
            id="LineE"
            className="collector-graphic--data data-e"
            d="M274.8,58.3c0,63-105.5,35.5-105.5,81.3"
          />
          <path
            id="LineD"
            className="collector-graphic--data data-d"
            d="M217.9,58.3c0,10.2-9.4,25.8-23.6,34.5s-29.5,24.4-29.5,46.7"
          />
          <line
            id="LineC"
            className="collector-graphic--data data-c"
            x1="161"
            y1="58.3"
            x2="161"
            y2="139.5"
          />
          <path
            id="LineB"
            className="collector-graphic--data data-b"
            d="M104.1,58.3c0,10.2,9.4,25.8,23.6,34.5s29.5,24.4,29.5,46.7"
          />
          <path
            id="LineA"
            className="collector-graphic--data data-a"
            d="M47.2,58.3c0,63,105.5,35.5,105.5,81.3"
          />
        </g>
        <g id="Dots">
          <circle
            id="DotE"
            className="collector-graphic--dot dot-e"
            cx="274.8"
            cy="51.3"
            r="7"
          />
          <circle
            id="DotD"
            className="collector-graphic--dot dot-d"
            cx="217.9"
            cy="51.3"
            r="7"
          />
          <circle
            id="DotC"
            className="collector-graphic--dot dot-c"
            cx="161"
            cy="51.3"
            r="7"
          />
          <circle
            id="DotB"
            className="collector-graphic--dot dot-b"
            cx="104.1"
            cy="51.3"
            r="7"
          />
          <circle
            id="DotA"
            className="collector-graphic--dot dot-a"
            cx="47.2"
            cy="51.3"
            r="7"
          />
        </g>
        <path
          id="BucketMask"
          className="collector-graphic--bucket"
          d="M176.2,126.8v2.3c5.9,0.9,9.7,2.2,9.7,3.7c0,2.6-11.2,4.7-25.1,4.7s-25.1-2.1-25.1-4.7
  c0-1.5,3.6-2.8,9.2-3.6V127c-7.7,1.3-12.7,3.4-12.7,5.9l6.9,51.1c0,3.9,9.7,7.1,21.8,7.1s21.8-3.1,21.8-7.1l6.9-51.1
  C189.7,130.3,184.3,128.1,176.2,126.8z"
        />
        <g id="Cubo" className="collector-graphic--cubo">
          <polygon
            className="collector-graphic--cubo-line"
            points="166.5,150.9 155.1,150.9 149.4,160.8 155.1,170.7 166.5,170.7 172.2,160.8 		"
          />
          <polygon
            className="collector-graphic--cubo-line"
            points="155.1,164.1 160.8,154.2 166.5,164.1 		"
          />
          <polyline
            className="collector-graphic--cubo-line"
            points="155.1,150.9 160.8,154.2 166.5,150.9 		"
          />
          <polyline
            className="collector-graphic--cubo-line"
            points="172.2,160.8 166.5,164.1 166.5,170.7 		"
          />
          <polyline
            className="collector-graphic--cubo-line"
            points="155.1,170.7 155.1,164.1 149.4,160.8 		"
          />
        </g>
      </svg>
    </div>
  )
}

export default CollectorGraphic
