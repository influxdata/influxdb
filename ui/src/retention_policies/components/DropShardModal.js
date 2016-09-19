import React, {PropTypes} from 'react';

const DropShardModal = React.createClass({
  propTypes: {
    onConfirm: PropTypes.func.isRequired,
  },

  getInitialState() {
    return {error: null, text: ''};
  },

  componentDidMount() {
    // Using this unfortunate hack because this modal is still using bootstrap,
    // and this component is never removed once being mounted -- meaning it doesn't
    // start with a new initial state when it gets closed/reopened.  A better
    // long term solution is just to handle modals in ReactLand.
    $('#dropShardModal').on('hide.bs.modal', () => { // eslint-disable-line no-undef
      this.setState({error: null, text: ''});
    });
  },

  handleConfirmationTextChange(e) {
    this.setState({text: e.target.value});
  },

  render() {
    return (
      <div className="modal fade" id="dropShardModal" tabIndex={-1} role="dialog" aria-labelledby="myModalLabel">
        <div className="modal-dialog" role="document">
          <div className="modal-content">
            <div className="modal-header">
              <button type="button" className="close" data-dismiss="modal" aria-label="Close">
                <span aria-hidden="true">×</span>
              </button>
              <h4 className="modal-title" id="myModalLabel">Are you sure?</h4>
            </div>
            <form onSubmit={this.handleSubmit}>
              <div className="modal-body">
                {this.state.error ?
                  <div className="alert alert-danger" role="alert">{this.state.error}</div>
                  : null}
                <div className="form-group col-md-12">
                  <label htmlFor="confirmation">All of the data on this shard will be removed permanently. Please Type 'delete' to confirm.</label>
                  <input onChange={this.handleConfirmationTextChange} value={this.state.text} type="text" className="form-control" id="confirmation" />
                </div>
              </div>
              <div className="modal-footer">
                <button type="button" className="btn btn-default" data-dismiss="modal">Cancel</button>
                <button ref="submitButton" type="submit" className="btn btn-danger">Delete</button>
              </div>
            </form>
          </div>
        </div>
      </div>
    );
  },

  handleSubmit(e) {
    e.preventDefault();

    if (this.state.text.toLowerCase() !== 'delete') {
      this.setState({error: "Please confirm by typing 'delete'"});
      return;
    }

    // Hiding the modal directly because we have an extra confirmation step,
    // bootstrap will close the modal immediately after clicking 'Delete'.
    $('#dropShardModal').modal('hide'); // eslint-disable-line no-undef

    this.props.onConfirm();
  },
});

export default DropShardModal;
