import React from 'react'
import PropTypes from 'prop-types'

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
