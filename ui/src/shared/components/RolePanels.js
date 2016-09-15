import React, {PropTypes} from 'react';
import {Link} from 'react-router';
import PermissionsTable from 'src/shared/components/PermissionsTable';
import {roleShape} from 'utils/propTypes';

const {arrayOf, string, bool, func} = PropTypes;
const RolePanels = React.createClass({
  propTypes: {
    roles: arrayOf(roleShape).isRequired,
    clusterID: string.isRequired,
    showUserCount: bool,
    onRemoveAccountFromRole: func,
  },

  getDefaultProps() {
    return {
      showUserCount: false,
    };
  },

  render() {
    const {roles, clusterID} = this.props;

    if (!roles.length) {
      return (
        <div className="panel panel-default">
          <div className="panel-body">
            <div className="generic-empty-state">
              <span className="icon alert-triangle"></span>
              <h4>This user has no roles</h4>
            </div>
          </div>
        </div>
      );
    }

    return (
      <div className="panel-group sub-page" role="tablist">
        {roles.map((role) => {
          const id = role.name.replace(/[^\w]/gi, '');
          return (
            <div key={role.name} className="panel panel-default">
              <div className="panel-heading" role="tab" id={`heading${id}`}>
                <h4 className="panel-title u-flex u-ai-center u-jc-space-between">
                  <a className="collapsed" role="button" data-toggle="collapse" href={`#collapse-role-${id}`}>
                    <span className="caret"></span>
                    {role.name}
                  </a>
                  <div>
                    {this.props.showUserCount ? <p>{role.users ? role.users.length : 0} Users</p> : null}
                    {this.props.onRemoveAccountFromRole ? (
                      <button
                        onClick={() => this.props.onRemoveAccountFromRole(role)}
                        data-toggle="modal"
                        data-target="#removeAccountFromRoleModal"
                        type="button"
                        className="btn btn-sm btn-link">
                        Remove
                      </button>
                    ) : null}
                    <Link to={`/clusters/${clusterID}/roles/${encodeURIComponent(role.name)}`} className="btn btn-xs btn-link">
                      Go To Role
                    </Link>
                  </div>
                </h4>
              </div>
              <div id={`collapse-role-${id}`} className="panel-collapse collapse" role="tabpanel">
                <PermissionsTable permissions={role.permissions} />
              </div>
            </div>
          );
        })}
      </div>
    );
  },
});

export default RolePanels;
