// Libraries
import React, {SFC} from 'react'

const LogoMongodb: SFC = () => {
  return (
    <svg width="100%" height="100%" viewBox="0 0 44.83 100">
      <defs>
        <linearGradient
          id="mongodb_a"
          x1={-960.8}
          y1={-1260.14}
          x2={-992.42}
          y2={-1260.35}
          gradientTransform="matrix(-.98 -.32 .29 -.88 -566.27 -1364.86)"
          gradientUnits="userSpaceOnUse"
        >
          <stop offset={0.23} stopColor="#999875" />
          <stop offset={0.56} stopColor="#9b9977" />
          <stop offset={0.68} stopColor="#a09f7e" />
          <stop offset={0.77} stopColor="#a9a889" />
          <stop offset={0.84} stopColor="#b7b69a" />
          <stop offset={0.9} stopColor="#c9c7b0" />
          <stop offset={0.95} stopColor="#deddcb" />
          <stop offset={0.99} stopColor="#f8f6eb" />
          <stop offset={1} stopColor="#fbf9ef" />
        </linearGradient>
        <linearGradient
          id="mongodb_b"
          x1={-955.93}
          y1={-1204.8}
          x2={-1001.42}
          y2={-1283.59}
          gradientTransform="matrix(-.98 -.32 .29 -.88 -566.27 -1364.86)"
          gradientUnits="userSpaceOnUse"
        >
          <stop offset={0} stopColor="#48a547" />
          <stop offset={1} stopColor="#3f9143" />
        </linearGradient>
        <linearGradient
          id="mongodb_c"
          x1={-951.77}
          y1={-1261.44}
          x2={-984.01}
          y2={-1239.78}
          gradientTransform="matrix(-.98 -.32 .29 -.88 -566.27 -1364.86)"
          gradientUnits="userSpaceOnUse"
        >
          <stop offset={0} stopColor="#41a247" />
          <stop offset={0.35} stopColor="#4ba74b" />
          <stop offset={0.96} stopColor="#67b554" />
          <stop offset={1} stopColor="#69b655" />
        </linearGradient>
      </defs>
      <title>{'logo_mongodb'}</title>
      <path
        d="M24.7 100l-2.7-.89s.34-13.57-4.55-14.52c-3.23-3.74.49-159.8 12.22-.54 0 0-4 2-4.77 5.44S24.7 100 24.7 100z"
        fill="url(#mongodb_a)"
      />
      <path
        d="M26.15 86.89S49.46 71.54 44 39.66C38.74 16.5 26.32 8.88 25 6a35 35 0 0 1-3-5.73l1 64.54s-2 19.7 3.15 22.08z"
        fill="url(#mongodb_b)"
      />
      <path
        d="M20.66 87.75S-1.21 72.83.05 46.52s16.7-39.26 19.71-41.6C21.7 2.84 21.78 2 21.93 0c1.37 2.93 1.14 43.73 1.29 48.49.56 18.57-1.03 35.75-2.56 39.26z"
        fill="url(#mongodb_c)"
      />
    </svg>
  )
}

export default LogoMongodb
