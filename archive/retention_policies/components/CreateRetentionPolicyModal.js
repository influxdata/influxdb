import React, {PropTypes} from 'react';

export default React.createClass({
  propTypes: {
    onCreate: PropTypes.func.isRequired,
    dataNodes: PropTypes.arrayOf(PropTypes.string).isRequired,
  },

  render() {
    return (
      <div className="modal fade" id="rpModal" tabIndex={-1} role="dialog" aria-labelledby="myModalLabel">
        <div className="modal-dialog" role="document">
          <div className="modal-content">
            <div className="modal-header">
              <button type="button" className="close" data-dismiss="modal" aria-label="Close">
                <span aria-hidden="true">×</span>
              </button>
              <h4 className="modal-title" id="myModalLabel">Create Retention Policy</h4>
            </div>
            <form onSubmit={this.handleSubmit}>
              <div className="modal-body">
                <div className="form-group col-md-12">
                  <label htmlFor="rpName">Name Retention Pollicy</label>
                  <input ref={(r) => this.rpName = r} type="text" className="form-control" id="rpName" placeholder="Name" required={true}/>
                </div>
                <div className="form-group col-md-6">
                  <label htmlFor="durationSelect">Select Duration</label>
                  <select ref={(r) => this.duration = r} className="form-control" id="durationSelect">
                    <option value="1d">1 Day</option>
                    <option value="7d">7 Days</option>
                    <option value="30d">30 Days</option>
                    <option value="365d">365 Days</option>
                  </select>
                </div>
                <div className="form-group col-md-6">
                  <label htmlFor="replicationFactor">Replication Factor</label>
                  <select ref={(r) => this.replicationFactor = r} className="form-control" id="replicationFactor">
                    {
                      this.props.dataNodes.map((node, i) => <option key={node}>{i + 1}</option>)
                    }
                  </select>
                </div>
              </div>
              <div className="modal-footer">
                <button type="button" className="btn btn-default" data-dismiss="modal">Cancel</button>
                <button ref="submitButton" type="submit" className="btn btn-success">Create</button>
              </div>
            </form>
          </div>
        </div>
      </div>
    );
  },

  handleSubmit(e) {
    e.preventDefault();
    const rpName = this.rpName.value;
    const duration = this.duration.value;
    const replicationFactor = this.replicationFactor.value;

    // Not using data-dimiss="modal" becuase it doesn't play well with HTML5 validations.
    $('#rpModal').modal('hide'); // eslint-disable-line no-undef

    this.props.onCreate({
      rpName,
      duration,
      replicationFactor,
    });
  },
});
