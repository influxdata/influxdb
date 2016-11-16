import React from 'react';
import {withRouter} from 'react-router';

const Login = React.createClass({
  render() {
    return (
      <a className="btn btn-primary" href="/oauth">Click me to log in</a>
    );
  },
});

export default withRouter(Login);
