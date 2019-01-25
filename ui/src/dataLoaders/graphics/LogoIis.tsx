// Libraries
import React, {SFC} from 'react'

const LogoIis: SFC = () => {
  return (
    <svg width="100%" height="100%" viewBox="0 0 100 100">
      <defs>
        <linearGradient
          id="iis_b"
          x1={-514.77}
          y1={316.77}
          x2={-514.77}
          y2={316.81}
          gradientTransform="matrix(2493.7 0 0 -2493.7 1283740.92 790028.16)"
          gradientUnits="userSpaceOnUse"
        >
          <stop offset={0} stopColor="#574c4a" />
          <stop offset={1} stopColor="#80716d" />
        </linearGradient>
        <linearGradient
          id="iis_c"
          x1={-514.92}
          y1={316.94}
          x2={-514.95}
          y2={316.99}
          gradientTransform="matrix(1567.75 0 0 -1504.18 807337.55 476821.08)"
          gradientUnits="userSpaceOnUse"
        >
          <stop offset={0} stopColor="#268d83" />
          <stop offset={1} stopColor="#2ea19e" />
        </linearGradient>
        <radialGradient
          id="iis_a"
          cx={-517.38}
          cy={323.85}
          r={0.02}
          gradientTransform="matrix(181.37 0 0 -181.37 93879.52 58811.53)"
          gradientUnits="userSpaceOnUse"
        >
          <stop offset={0} stopColor="#db7c7c" />
          <stop offset={1} stopColor="#c83737" />
        </radialGradient>
        <radialGradient
          id="iis_d"
          cx={-519.14}
          cy={323.85}
          r={0.02}
          gradientTransform="matrix(181.36 0 0 -181.36 94206.95 58808.39)"
          xlinkHref="#iis_a"
        />
      </defs>
      <title>{'logo_iis'}</title>
      <path
        d="M0 67.57V32.43C0 4.05 4.05 0 32.4 0h35.2C96 0 100 4.05 100 32.43v35.14C100 96 96 100 67.6 100H32.4C4.05 100 0 96 0 67.57z"
        fill="url(#iis_b)"
      />
      <path
        d="M21.58 18.85a279.62 279.62 0 0 0-2.34 60.32H34.6c-1.46-7.78-6.7-43.31-2.34-43.43 2.34.37 13 30.15 13 30.15a42.66 42.66 0 0 1 4.72-.3 42.66 42.66 0 0 1 4.72.3s10.68-29.78 13-30.15c4.36.12-.88 35.65-2.34 43.43h15.4a279.62 279.62 0 0 0-2.34-60.32H64.19c-2.7 0-13 18.1-14.19 18.1s-11.48-18.07-14.19-18.1z"
        fill="url(#iis_c)"
      />
      <path
        d="M47 75.53a3.64 3.64 0 1 1-3.64-3.64A3.64 3.64 0 0 1 47 75.53z"
        fill="url(#iis_a)"
      />
      <path
        d="M60.3 75.53a3.64 3.64 0 1 1-3.63-3.64 3.63 3.63 0 0 1 3.63 3.64z"
        fill="url(#iis_d)"
      />
      <path
        d="M77.69 19.88A272.7 272.7 0 0 1 80.39 60c0 11-.67 18.11-.67 18.11H66.79l-1.39 1h15.36a279.62 279.62 0 0 0-2.34-60.32l-.73 1zM37 19.5c4 4.5 11 16.41 12 16.41-2.6-3.28-8.89-13.72-12-16.41zm-5.78 15.2c-4.36.12.88 35.65 2.34 43.43H20.15l-.91 1H34.6c-1.45-7.74-6.65-43-2.41-43.43-.39-.59-.73-1-1-1zm35.48 0c-2.34.37-13 30.14-13 30.14a44.1 44.1 0 0 0-4.7-.29c-1.4 0-2.61.09-3.42.16l-.26 1.18a42.66 42.66 0 0 1 4.72-.3 42.66 42.66 0 0 1 4.72.3s10.6-29.58 13-30.15c-.26-.65-.58-1-1-1z"
        style={{
          isolation: 'isolate',
        }}
        opacity={0.1}
      />
      <path
        d="M21.58 18.85a279.62 279.62 0 0 0-2.34 60.32l.9-1a279.63 279.63 0 0 1 2.48-58.26h14.23a2.21 2.21 0 0 1 1.17.65c-.93-1-1.7-1.69-2.21-1.69zm42.61 0c-2.7 0-13 18.1-14.19 18.1.48.61.88 1 1 1 1.19 0 11.49-18.07 14.19-18.1h12.6l.63-1zM33.24 36.78c3.45 5.19 12 29.11 12 29.11l.25-1.17c-2.12-5.81-10.2-27.61-12.23-27.94zm35.54 0c2.32 5.92-2.07 35.39-3.38 42.39l1.39-1.09c1.78-10.44 6.06-41.19 1.99-41.3z"
        style={{
          isolation: 'isolate',
        }}
        fill="#fff"
        opacity={0.3}
      />
    </svg>
  )
}

export default LogoIis
