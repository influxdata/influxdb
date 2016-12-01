import React, {PropTypes} from 'react';

const RenamePanelModal = React.createClass({
  propTypes: {
    onConfirm: PropTypes.func.isRequired,
    panel: PropTypes.shape({
      id: PropTypes.string.isRequired,
    }),
  },

  getInitialState() {
    return {error: null};
  },

  componentDidMount() {
    this.refs.name.focus();
  },

  render() {
    const {panel} = this.props;

    return (
      <div className="modal fade in" id={`renamePanelModal-${panel.id}`} tabIndex="-1" role="dialog">
        <div className="modal-dialog">
          <div className="modal-content">
            <div className="modal-header">
              <button type="button" className="close" data-dismiss="modal" aria-label="Close">
                <span aria-hidden="true">×</span>
              </button>
              <h4 className="modal-title">Rename Panel</h4>
            </div>
            <div className="modal-body">
              {this.state.error ?
                <div className="alert alert-danger" role="alert">{this.state.error}</div>
                : null}
              <div className="form-grid padding-top">
                <div className="form-group col-md-8 col-md-offset-2">
                  <input ref="name" name="renameExplorer" type="text" placeholder={panel.name} className="form-control input-lg" id="renameExplorer" required={true} />
                </div>
              </div>
            </div>
            <div className="modal-footer">
              <button className="btn btn-info" data-dismiss="modal">Cancel</button>
              <button onClick={this.handleConfirm} className="btn btn-success">Rename</button>
            </div>
          </div>
        </div>
      </div>
    );
  },

  handleConfirm() {
    const name = this.refs.name.value;

    if (name === '') {
      this.setState({error: "Name can't be blank"});
      return;
    }

    $(`#renamePanelModal-${this.props.panel.id}`).modal('hide'); // eslint-disable-line no-undef
    this.refs.name.value = '';
    this.setState({error: null});
    this.props.onConfirm(name);
  },
});

export default RenamePanelModal;
