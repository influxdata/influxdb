// Libraries
import React, {SFC} from 'react'

const KotlinLogo: SFC = () => {
  return (
    <svg x={0} y={0} viewBox="0 0 60 60" xmlSpace="preserve" width="80" height="100">
    <linearGradient
      id="kotlin-a"
      gradientUnits="userSpaceOnUse"
      x1={15.959}
      y1={-13.014}
      x2={44.307}
      y2={15.333}
      gradientTransform="matrix(1 0 0 -1 0 61)"
    >
      <stop offset={0.097} stopColor="#0095d5" />
      <stop offset={0.301} stopColor="#238ad9" />
      <stop offset={0.621} stopColor="#557bde" />
      <stop offset={0.864} stopColor="#7472e2" />
      <stop offset={1} stopColor="#806ee3" />
    </linearGradient>
    <path fill="url(#kotlin-a)" d="M0 60L30.1 29.9 60 60z" />
    <linearGradient
      id="kotlin-b"
      gradientUnits="userSpaceOnUse"
      x1={4.209}
      y1={48.941}
      x2={20.673}
      y2={65.405}
      gradientTransform="matrix(1 0 0 -1 0 61)"
    >
      <stop offset={0.118} stopColor="#0095d5" />
      <stop offset={0.418} stopColor="#3c83dc" />
      <stop offset={0.696} stopColor="#6d74e1" />
      <stop offset={0.833} stopColor="#806ee3" />
    </linearGradient>
    <path fill="url(#kotlin-b)" d="M0 0L30.1 0 0 32.5z" />
    <linearGradient
      id="kotlin-c"
      gradientUnits="userSpaceOnUse"
      x1={-10.102}
      y1={5.836}
      x2={45.731}
      y2={61.669}
      gradientTransform="matrix(1 0 0 -1 0 61)"
    >
      <stop offset={0.107} stopColor="#c757bc" />
      <stop offset={0.214} stopColor="#d0609a" />
      <stop offset={0.425} stopColor="#e1725c" />
      <stop offset={0.605} stopColor="#ee7e2f" />
      <stop offset={0.743} stopColor="#f58613" />
      <stop offset={0.823} stopColor="#f88909" />
    </linearGradient>
    <path fill="url(#kotlin-c)" d="M30.1 0L0 31.7 0 60 30.1 29.9 60 0z" />
  </svg>
  )
}

export default KotlinLogo
