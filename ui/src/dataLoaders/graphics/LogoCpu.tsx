// Libraries
import React, {SFC} from 'react'

const LogoCpu: SFC = () => {
  return (
    <svg
      width="100%"
      height="100%"
      id="Layer_1"
      xmlns="http://www.w3.org/2000/svg"
      viewBox="0 0 100 100"
    >
      <g id="cpu_icon">
        <style>
          {`
          .cpu_a{fill:#7A65F1;}
          .cpu_b{fill:none;stroke:#2C323D;stroke-width:7;stroke-linejoin:round;stroke-miterlimit:10;}
          .cpu_c{fill:#2C323D;}
          `}
        </style>
        <path
          className="cpu_a"
          d="M96,100H4c-2.2,0-4-1.8-4-4V4c0-2.2,1.8-4,4-4h92c2.2,0,4,1.8,4,4v92C100,98.2,98.2,100,96,100z"
        />
        <line className="cpu_b" x1="22.2" y1="100" x2="22.2" />
        <line className="cpu_b" x1="40.7" y1="100" x2="40.7" />
        <line className="cpu_b" x1="59.3" y1="100" x2="59.3" />
        <line className="cpu_b" x1="77.8" y1="100" x2="77.8" />
        <line className="cpu_b" y1="22.2" x2="100" y2="22.2" />
        <line className="cpu_b" y1="40.7" x2="100" y2="40.7" />
        <line className="cpu_b" y1="59.3" x2="100" y2="59.3" />
        <line className="cpu_b" y1="77.8" x2="100" y2="77.8" />
        <g>
          <rect x="17" y="17" className="cpu_a" width="66" height="66" />
          <path
            className="cpu_c"
            d="M76,24v52H24V24H76 M88,10H12c-1.1,0-2,0.9-2,2v76c0,1.1,0.9,2,2,2h76c1.1,0,2-0.9,2-2V12 C90,10.9,89.1,10,88,10L88,10z"
          />
        </g>
      </g>
    </svg>
  )
}

export default LogoCpu
