import React, {PropTypes} from 'react';
import SideNav from '../components/SideNav';
import {getMe} from 'src/shared/apis';

const {func, string} = PropTypes;
const SideNavApp = React.createClass({
  propTypes: {
    currentLocation: string.isRequired,
    addFlashMessage: func.isRequired,
    sourceID: string.isRequired,
    explorationID: string,
  },

  getInitialState() {
    return {
      me: null,
    };
  },

  componentDidMount() {
    getMe().then(({data: me}) => {
      this.setState({me});
    }).catch(({response: {status}}) => {
      const NO_AUTH_ENABLED = 418;
      if (status !== NO_AUTH_ENABLED) {
        this.props.addFlashMessage({type: 'error', text: 'There was a network problem while retrieving the user'});
      }
    });
  },

  render() {
    const {currentLocation, sourceID, explorationID} = this.props;
    const {me} = this.state;

    return (
      <SideNav
        sourceID={sourceID}
        location={currentLocation}
        explorationID={explorationID}
        me={me}
      />
    );
  },

});

export default SideNavApp;
