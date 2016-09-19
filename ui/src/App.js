import React, {PropTypes} from 'react';
import FlashMessages from 'shared/components/FlashMessages';
import SideNavContainer from 'src/side_nav';

const App = React.createClass({
  propTypes: {
    params: PropTypes.shape({
      clusterID: PropTypes.string.isRequired,
    }).isRequired,
    addFlashMessage: PropTypes.func.isRequired, // Injected by the `FlashMessages` wrapper
    children: PropTypes.node.isRequired,
    location: PropTypes.shape({
      pathname: PropTypes.string,
    }),
  },

  render() {
    const {clusterID} = this.props.params;

    return (
      <div className="enterprise-wrapper--flex">
        <SideNavContainer addFlashMessage={this.props.addFlashMessage} clusterID={clusterID} currentLocation={this.props.location.pathname} />
        <div className="page-wrapper">
          {this.props.children && React.cloneElement(this.props.children, {
            addFlashMessage: this.props.addFlashMessage,
          })}
        </div>
      </div>
    );
  },
});

export default FlashMessages(App);
