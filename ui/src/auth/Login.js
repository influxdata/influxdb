import React from 'react';
import {withRouter} from 'react-router';

const Login = React.createClass({
  render() {
    return (
      <div className="auth-page">
        <div className="auth-box">
          <div className="auth-logo"></div>
          <h1 className="auth-text-logo">Chronograf</h1>
          <p><strong>v1.2-beta6</strong> / Time-Series Data Visualization</p>
          <a className="btn btn-primary" href="/oauth/github/login"><span className="icon github"></span> Login with GitHub</a>
        </div>
        <p className="auth-credits">Made by <span className="icon cubo-uniform"></span>InfluxData</p>
        <div className="auth-image"></div>
      </div>
    );
  },
});

export default withRouter(Login);
