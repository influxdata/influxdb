import React, {PropTypes} from 'react';
import FlashMessages from 'shared/components/FlashMessages';
import SideNavContainer from 'src/side_nav';

const App = React.createClass({
  propTypes: {
    addFlashMessage: PropTypes.func.isRequired, // Injected by the `FlashMessages` wrapper
    children: PropTypes.node.isRequired,
    location: PropTypes.shape({
      pathname: PropTypes.string,
    }),
  },

  render() {
    return (
      <div className="enterprise-wrapper--flex">
        <SideNavContainer addFlashMessage={this.props.addFlashMessage} currentLocation={this.props.location.pathname} />
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
