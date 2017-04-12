/* global VERSION */
import React, {PropTypes} from 'react'
import {connect} from 'react-redux'

import Notifications from 'shared/components/Notifications'

const Login = ({auth, location}) => {
  if (auth.isAuthLoading) {
    return <div className="page-spinner"></div>
  }

  return (
    <div>
      <Notifications location={location} />
      <div className="auth-page">
        <div className="auth-box">
          <div className="auth-logo"></div>
          <h1 className="auth-text-logo">Chronograf</h1>
          <p><strong>{VERSION}</strong> / Time-Series Data Visualization</p>
          {auth.links.map(({name, login, label}) => (
            <a key={name} className="btn btn-primary" href={login}>
              <span className={`icon ${name}`}></span>
              Login with {label}
            </a>
          ))}
        </div>
        <p className="auth-credits">Made by <span className="icon cubo-uniform"></span>InfluxData</p>
        <div className="auth-image"></div>
      </div>
    </div>
  )
}

const {
  array,
  bool,
  shape,
  string,
} = PropTypes

Login.propTypes = {
  auth: shape({
    me: shape(),
    links: array,
    isLoading: bool,
  }),
  location: shape({
    pathname: string,
  }),
}

const mapStateToProps = ({auth}) => ({
  auth,
})

export default connect(mapStateToProps)(Login)
