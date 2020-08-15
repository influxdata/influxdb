// Libraries
import React, {SFC} from 'react'

const RubyLogo: SFC = () => {
  return (
    <svg
      version="1.1"
      id="Layer_1"
      viewBox="0 0 48 48"
      enableBackground="new 0 0 48 48"
      xmlSpace="preserve"
      preserveAspectRatio="xMidYMid meet"
    >
      <polygon fill="#9B1010" points="42,14 40,40 14,42 " />
      <polygon fill="#B71C1C" points="28,28 40,40 42,14 " />
      <ellipse
        transform="matrix(0.7071 -0.7071 0.7071 0.7071 -9.9411 24)"
        fill="#C62828"
        cx="24"
        cy="24"
        rx="22.621"
        ry="11.664"
      />
      <polygon
        fill="#E53935"
        points="10,17 17,10 25,6 31,10 28,19 19,27 10,30 6,24 "
      />
      <path
        fill="#FF5252"
        d="M31,10l-6-4h11L31,10z M42,15l-11-5l-3,9L42,15z M19,27l13.235,5.235L28,19L19,27z M10,30l4,12l5-15L10,30z
   M6,24v11l4-5L6,24z"
      />
    </svg>
  )
}

export default RubyLogo
