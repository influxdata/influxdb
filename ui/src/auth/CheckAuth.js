import React, {PropTypes} from 'react';
import {withRouter} from 'react-router';

const CheckAuth = React.createClass({
  propTypes: {
    router: PropTypes.shape({
      push: PropTypes.func.isRequired,
    }).isRequired,
    children: PropTypes.node.isRequired,
  },

  getInitialState() {
    return {
      loggedIn: false,
    };
  },

  componentDidMount() {
    // console.log('checking auth');
    // this.setState({
    //   loggedIn: true,
    // });
    this.props.router.push('/login');
  },

  render() {
    const {loggedIn} = this.state;
    if (!loggedIn) {
      return <div>AUTH IS BEING CHECKED</div>;
    }

    return this.props.children;
  },
});

export default withRouter(CheckAuth);
