// Libraries
import React, {SFC} from 'react'

const ScalaLogo: SFC = () => {
  return (
    <svg viewBox="0 0 64 64" height={100} width={100}>
    <linearGradient id="scala-a">
      <stop offset={0} stopColor="#656565" />
      <stop offset={1} stopColor="#010101" />
    </linearGradient>
    <linearGradient
      id="scala-c"
      gradientUnits="userSpaceOnUse"
      x1={13.528}
      x2={88.264}
      xlinkHref="#scala-a"
      y1={-36.176}
      y2={-36.176}
    />
    <linearGradient
      id="scala-d"
      gradientUnits="userSpaceOnUse"
      x1={13.528}
      x2={88.264}
      xlinkHref="#scala-a"
      y1={3.91}
      y2={3.91}
    />
    <linearGradient id="scala-b">
      <stop offset={0} stopColor="#9f1c20" />
      <stop offset={1} stopColor="#ed2224" />
    </linearGradient>
    <linearGradient
      id="scala-e"
      gradientUnits="userSpaceOnUse"
      x1={13.528}
      x2={88.264}
      xlinkHref="#scala-b"
      y1={-55.974}
      y2={-55.974}
    />
    <linearGradient
      id="scala-f"
      gradientUnits="userSpaceOnUse"
      x1={13.528}
      x2={88.264}
      xlinkHref="#scala-b"
      y1={-15.87}
      y2={-15.87}
    />
    <linearGradient
      id="scala-g"
      gradientUnits="userSpaceOnUse"
      x1={13.528}
      x2={88.264}
      xlinkHref="#scala-b"
      y1={24.22}
      y2={24.22}
    />
    <path
      d="M13.4-31s75 7.5 75 20v-30s0-12.5-75-20z"
      fill="url(#scala-d)"
      transform="matrix(.4923 0 0 .4923 6.942 39.877)"
    />
    <path
      d="M13.4 9s75 7.5 75 20V-1s0-12.5-75-20z"
      fill="url(#scala-d)"
      transform="matrix(.4923 0 0 .4923 6.942 39.877)"
    />
    <path
      d="M88.4-81v30s0 12.5-75 20v-30s75-7.5 75-20"
      fill="url(#scala-e)"
      transform="matrix(.4923 0 0 .4923 6.942 39.877)"
    />
    <path
      d="M13.4-21s75-7.5 75-20v30s0 12.5-75 20z"
      fill="url(#scala-f)"
      transform="matrix(.4923 0 0 .4923 6.942 39.877)"
    />
    <path
      d="M13.4 49V19s75-7.5 75-20v30s0 12.5-75 20"
      fill="url(#scala-g)"
      transform="matrix(.4923 0 0 .4923 6.942 39.877)"
    />
  </svg>
  )
}

export default ScalaLogo
