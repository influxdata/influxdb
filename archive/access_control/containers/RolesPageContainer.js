import React, {PropTypes} from 'react';
import {getRoles, createRole} from 'src/shared/apis';
import {buildRoles} from 'src/shared/presenters';
import RolesPage from '../components/RolesPage';
import _ from 'lodash';

export const RolesPageContainer = React.createClass({
  propTypes: {
    params: PropTypes.shape({
      clusterID: PropTypes.string.isRequired,
    }).isRequired,
    addFlashMessage: PropTypes.func,
  },

  getInitialState() {
    return {
      roles: [],
    };
  },

  componentDidMount() {
    this.fetchRoles();
  },

  fetchRoles() {
    getRoles(this.props.params.clusterID).then((resp) => {
      this.setState({
        roles: buildRoles(resp.data.roles),
      });
    }).catch((err) => {
      console.error(err.toString()); // eslint-disable-line no-console
      this.props.addFlashMessage({
        type: 'error',
        text: `Unable to fetch roles! Please try refreshing the page.`,
      });
    });
  },

  handleCreateRole(roleName) {
    createRole(this.props.params.clusterID, roleName)
      // TODO: this should be an optimistic update, but we can't guarantee that we'll
      // get an error when a user tries to make a duplicate role (we don't want to
      // display a role twice). See https://github.com/influxdata/plutonium/issues/538
      .then(this.fetchRoles)
      .then(() => {
        this.props.addFlashMessage({
          type: 'success',
          text: 'Role created!',
        });
      })
      .catch((err) => {
        const text = _.result(err, ['response', 'data', 'error', 'toString'], 'An error occurred.');
        this.props.addFlashMessage({
          type: 'error',
          text,
        });
      });
  },

  render() {
    return (
      <RolesPage
        roles={this.state.roles}
        onCreateRole={this.handleCreateRole}
        clusterID={this.props.params.clusterID}
      />
    );
  },
});

export default RolesPageContainer;
