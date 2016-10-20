import React, {PropTypes} from 'react';
import {connect} from 'react-redux';
import SideNavContainer from 'src/side_nav';
import {
  publishNotification as publishNotificationAction,
  dismissNotification as dismissNotificationAction,
} from 'src/shared/actions/notifications';

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
    dismissNotification: PropTypes.func.isRequired,
    notifications: PropTypes.shape({
      success: PropTypes.string,
      error: PropTypes.string,
      warning: PropTypes.string,
    }),
  },

  handleNotification({type, text}) {
    this.props.publishNotification(type, text);
  },

  handleDismissNotification(type) {
    this.props.dismissNotification(type);
  },

  render() {
    const {sourceID} = this.props.params;

    return (
      <div className="enterprise-wrapper--flex">
        <SideNavContainer sourceID={sourceID} addFlashMessage={this.handleNotification} currentLocation={this.props.location.pathname} />
        <div className="page-wrapper">
          {this.renderNotifications()}
          {this.props.children && React.cloneElement(this.props.children, {
            addFlashMessage: this.handleNotification,
          })}
        </div>
      </div>
    );
  },

  renderNotifications() {
    const {success, error} = this.props.notifications;
    if (!success && !error) {
      return null;
    }
    if (success) {
      return (
        <div className="flash-messages">
          <div className="alert alert-success" role="alert">
            {success}{this.renderDismiss()}
          </div>
        </div>
      );
    }
    if (error) {
      return (
        <div className="flash-messages">
          <div className="alert alert-danger" role="alert">
            {error}{this.renderDismiss()}
          </div>
        </div>
      );
    }
  },

  renderDismiss() {
    return (
      <button className="close" data-dismiss="alert" aria-label="Close" onClick={this.handleDismissNotification}>
        <span className="icon remove"></span>
      </button>
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
  dismissNotification: dismissNotificationAction,
})(App);
