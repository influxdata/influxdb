/* global VERSION */
import React, {PropTypes} from 'react'

import Notifications from 'shared/components/Notifications'

import SplashPage from 'shared/components/SplashPage'

const Login = ({authData: {auth}}) => {
  if (auth.isAuthLoading) {
    return <div className="page-spinner" />
  }

  return (
    <div>
      <Notifications />
      <SplashPage>
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
      </SplashPage>
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
