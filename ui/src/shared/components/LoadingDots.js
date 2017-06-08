import React, {PropTypes} from 'react'

const LoadingDots = ({className}) =>
  <div className={`loading-dots ${className}`}>
    <div />
    <div />
    <div />
  </div>

const {string} = PropTypes

LoadingDots.propTypes = {
  className: string,
}

export default LoadingDots
