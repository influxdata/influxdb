import React, {PropTypes} from 'react'

const SplashPage = ({children}) =>
  <div className="auth-page">
    <div className="auth-box">
      <div className="auth-logo" />
      {children}
    </div>
    <p className="auth-credits">
      Made by <span className="icon cubo-uniform" />InfluxData
    </p>
    <div className="auth-image" />
  </div>

const {node} = PropTypes
SplashPage.propTypes = {
  children: node,
}

export default SplashPage
