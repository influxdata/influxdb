/* global VERSION */
import React, {PropTypes} from 'react'

import Notifications from 'shared/components/Notifications'

const Login = ({authData: {auth}}) => {
  if (auth.isAuthLoading) {
    return <div className="page-spinner" />
  }

  return (
    <div>
      <Notifications />
      <div className="auth-page">
        <div className="auth-box">
          <div className="auth-logo" />
          <h1 className="auth-text-logo">Chronograf</h1>
          <p>
            <strong>{VERSION}</strong> / Time-Series Data Visualization
          </p>
          {auth.links &&
            auth.links.map(({name, login, label}) =>
              <a key={name} className="btn btn-primary" href={login}>
                <span className={`icon ${name}`} />
                Login with {label}
              </a>
            )}
        </div>
        <p className="auth-credits">
          Made by <span className="icon cubo-uniform" />InfluxData
        </p>
        <div className="auth-image" />
      </div>
    </div>
  )
}

const {array, bool, shape, string} = PropTypes

Login.propTypes = {
  authData: shape({
    me: shape(),
    links: array,
    isLoading: bool,
  }),
  location: shape({
    pathname: string,
  }),
}

export default Login
