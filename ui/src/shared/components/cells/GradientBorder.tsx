// Libraries
import React, {SFC} from 'react'

const GradientBorder: SFC = () => (
  <div className="gradient-border">
    <div className="gradient-border--top-left" />
    <div className="gradient-border--top-right" />
    <div className="gradient-border--bottom-left" />
    <div className="gradient-border--bottom-right" />
  </div>
)

export default GradientBorder
