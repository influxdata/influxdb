import React, {PropTypes} from 'react';

const AttachWebUsers = React.createClass({
  propTypes: {
    users: PropTypes.arrayOf(PropTypes.shape()).isRequired,
    account: PropTypes.string.isRequired,
    onConfirm: PropTypes.func.isRequired,
  },

  getInitialState() {
    return {
      selectedUsers: [],
    };
  },

  handleConfirm() {
    $('#addWebUsers').modal('hide'); // eslint-disable-line no-undef
    this.props.onConfirm(this.state.selectedUsers);
    // uncheck all the boxes?
  },

  handleSelection(e) {
    const checked = e.target.checked;
    const id = parseInt(e.target.dataset.id, 10);
    const user = this.props.users.find((u) => u.id === id);
    const newSelectedUsers = this.state.selectedUsers.slice(0);

    if (checked) {
      newSelectedUsers.push(user);
    } else {
      const userIndex = newSelectedUsers.find(u => u.id === id);
      newSelectedUsers.splice(userIndex, 1);
    }

    this.setState({selectedUsers: newSelectedUsers});
  },

  render() {
    return (
      <div className="modal fade" id="addWebUsers" tabIndex="-1" role="dialog">
        <div className="modal-dialog">
          <div className="modal-content">
            <div className="modal-header">
              <button type="button" className="close" data-dismiss="modal" aria-label="Close">
                <span aria-hidden="true">×</span>
              </button>
              <h4 className="modal-title">
                Link Web Users to <strong>{this.props.account}</strong>
              </h4>
            </div>
            <div className="row">
              <div className="col-xs-10 col-xs-offset-1">
                <h4>Web Users</h4>
                <div className="well well-white">
                  <table className="table v-center">
                    <tbody>
                      { // TODO: style this and make it select / collect users
                        this.props.users.map((u) => {
                          return (
                            <tr key={u.name}>
                              <td><input onChange={this.handleSelection} data-id={u.id} type="checkbox" /></td>
                              <td>{u.name}</td>
                            </tr>
                          );
                        })
                      }
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
            <div className="modal-footer">
              <button className="btn btn-default" type="button" data-dismiss="modal">Cancel</button>
              <button onClick={this.handleConfirm} className="btn btn-success">Link Users</button>
            </div>
          </div>
        </div>
      </div>
    );
  },
});

export default AttachWebUsers;
