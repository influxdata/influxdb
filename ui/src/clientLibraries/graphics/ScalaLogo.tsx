// Libraries
import React, {SFC} from 'react'

const ScalaLogo: SFC = () => {
  return (
    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 350 550">
      <defs>
        <linearGradient id="scala-b" x1="0%" y1="0%" x2="100%" y2="0%">
          <stop offset="0%" stopColor="#a00000" />
          <stop offset="100%" stopColor="red" />
        </linearGradient>
        <linearGradient id="scala-a" x1="0%" y1="0%" x2="100%" y2="0%">
          <stop offset="0%" stopColor="#646464" />
          <stop offset="100%" />
        </linearGradient>
      </defs>
      <g fill="url(#scala-a)">
        <path d="M30 200s300 30 300 80V160s0-50-300-80V-40z" />
        <path d="M30 360s300 30 300 80V320s0-50-300-80V120z" />
      </g>
      <g fill="url(#scala-b)">
        <path d="M30 80S330 50 330 0v120s0 50-300 80v120z" />
        <path d="M30 240s300-30 300-80v120s0 50-300 80v120z" />
        <path d="M30 400s300-30 300-80v120s0 50-300 80v120z" />
      </g>
    </svg>
  )
}

export default ScalaLogo
