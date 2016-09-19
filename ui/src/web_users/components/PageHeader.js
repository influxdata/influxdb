import React, {PropTypes} from 'react';

const {string} = PropTypes;
const Header = React.createClass({
  propTypes: {
    activeCluster: string.isRequired,
  },

  render() {
    return (
      <div id="user-index-page" className="js-user-index">
        <div className="enterprise-header">
          <div className="enterprise-header__container">
            <div className="enterprise-header__left">
              <h1>
                Web Users
              </h1>
            </div>
            <div className="enterprise-header__right">
              <button className="btn btn-sm btn-primary" data-toggle="modal" data-target="#createUserModal">Invite User</button>
            </div>
          </div>
        </div>
      </div>
    );
  },
});

export default Header;
