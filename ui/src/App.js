import React, {PropTypes} from 'react';
import {connect} from 'react-redux';
import SideNavContainer from 'src/side_nav';
import {publishNotification as publishNotificationAction} from 'src/shared/actions/notifications';

const App = React.createClass({
  propTypes: {
    children: PropTypes.node.isRequired,
    location: PropTypes.shape({
      pathname: PropTypes.string,
    }),
    params: PropTypes.shape({
      sourceID: PropTypes.string.isRequired,
    }).isRequired,
    publishNotification: PropTypes.func.isRequired,
    notifications: PropTypes.shape({
      success: PropTypes.string,
      failure: PropTypes.string,
      warning: PropTypes.string,
    }),
  },

  componentDidMount() {
    const {publishNotification} = this.props;
    setTimeout(() => {
      publishNotification('success', 'everything worked :)');
    }, 500);
  },

  render() {
    const {sourceID} = this.props.params;
    console.log(this.props.notifications);
    const addFlashMessage = console.log;

    return (
      <div className="enterprise-wrapper--flex">
        <SideNavContainer sourceID={sourceID} addFlashMessage={addFlashMessage} currentLocation={this.props.location.pathname} />
        <div className="page-wrapper">
          {this.props.children && React.cloneElement(this.props.children, {
            addFlashMessage,
          })}
        </div>
      </div>
    );
  },
});

function mapStateToProps(state) {
  return {
    notifications: state.notifications,
  };
}

export default connect(mapStateToProps, {
  publishNotification: publishNotificationAction,
})(App);
