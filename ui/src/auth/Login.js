/* global VERSION */
import React, {PropTypes} from 'react'
import {connect} from 'react-redux'

const {array} = PropTypes

const Login = ({auths}) => (
  <div className="auth-page">
    <div className="auth-box">
      <div className="auth-logo"></div>
      <h1 className="auth-text-logo">Chronograf</h1>
      <p><strong>{VERSION}</strong> / Time-Series Data Visualization</p>
      {auths.map(({name, login, label}) => (
        <a key={name} className="btn btn-primary" href={login}>
          <span className={`icon ${name}`}></span>
          Login with {label}
        </a>
      ))}
    </div>
    <p className="auth-credits">Made by <span className="icon cubo-uniform"></span>InfluxData</p>
    <div className="auth-image"></div>
  </div>
)

Login.propTypes = {
  auths: array.isRequired,
}

const mapStateToProps = (state) => {
  return {
    auths: state.auth,
  }
}

export default connect(mapStateToProps)(Login)
