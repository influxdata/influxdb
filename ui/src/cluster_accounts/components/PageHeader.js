import React, {PropTypes} from 'react';
import CreateAccountModal from './CreateAccountModal';

const Header = React.createClass({
  propTypes: {
    onCreateAccount: PropTypes.func,
    roles: PropTypes.arrayOf(PropTypes.shape({
      name: PropTypes.shape,
    })),
  },

  render() {
    const {roles, onCreateAccount} = this.props;

    return (
      <div id="cluster-accounts-page">
        <div className="enterprise-header">
          <div className="enterprise-header__container">
            <div className="enterprise-header__left">
              <h1>
                Access Control
              </h1>
            </div>
            <div className="enterprise-header__right">
              <button className="btn btn-sm btn-primary" data-toggle="modal" data-target="#createAccountModal" data-test="create-cluster-account">
                Create Cluster Account
              </button>
            </div>
          </div>
        </div>
        <CreateAccountModal roles={roles} onCreateAccount={onCreateAccount} />
      </div>
    );
  },
});

export default Header;
