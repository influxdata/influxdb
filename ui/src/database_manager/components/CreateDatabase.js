import React, {PropTypes} from 'react';

const {arrayOf, number, func} = PropTypes;
const CreateDatabase = React.createClass({
  propTypes: {
    replicationFactors: arrayOf(number.isRequired).isRequired,
    onCreateDatabase: func.isRequired,
  },

  getInitialState() {
    return {
      rpName: '',
      database: '',
      duration: '24h',
      replicaN: '1',
    };
  },

  handleRpNameChange(e) {
    this.setState({rpName: e.target.value});
  },

  handleDatabaseNameChange(e) {
    this.setState({database: e.target.value});
  },

  handleSelectDuration(e) {
    this.setState({duration: e.target.value});
  },

  handleSelectReplicaN(e) {
    this.setState({replicaN: e.target.value});
  },

  handleSubmit() {
    const {rpName, database, duration, replicaN} = this.state;
    this.props.onCreateDatabase({rpName, database, duration, replicaN});
  },


  render() {
    const {database, rpName, duration, replicaN} = this.state;

    return (
      <div className="modal fade" id="dbModal" tabIndex="-1" role="dialog" aria-labelledby="myModalLabel">
        <form data-remote="true" onSubmit={this.handleSubmit} >
          <div className="modal-dialog" role="document">
            <div className="modal-content">
              <div className="modal-header">
                <button type="button" className="close" data-dismiss="modal" aria-label="Close">
                  <span aria-hidden="true">×</span>
                </button>
                <h4 className="modal-title" id="myModalLabel">Create Database</h4>
              </div>
              <div className="modal-body">
                <div id="form-errors"></div>
                <div className="form-group col-sm-12">
                  <label htmlFor="name">Database Name</label>
                  <input onChange={this.handleDatabaseNameChange} value={database} required={true} className="form-control input-lg" type="text" id="name" name="name"/>
                </div>
                <div className="form-group col-sm-6">
                  <label htmlFor="retention-policy">Retention Policy Name</label>
                  <input onChange={this.handleRpNameChange} value={rpName} required={true} className="form-control input-lg" type="text" id="retention-policy" name="retention-policy"/>
                </div>
                <div className="form-group col-sm-3">
                  <label htmlFor="duration" data-toggle="tooltip" data-placement="top" title="How long InfluxDB stores data">Duration</label>
                  <select onChange={this.handleSelectDuration} defaultValue={duration} className="form-control input-lg" name="duration" id="exampleSelect" required={true}>
                    <option value="24h">1 Day</option>
                    <option value="168h">7 Days</option>
                    <option value="720h">30 Days</option>
                    <option value="8670h">365 Days</option>
                  </select>
                </div>
                <div className="form-group col-sm-3">
                  <label htmlFor="replication-factor" data-toggle="tooltip" data-placement="top" title="How many copies of the data InfluxDB stores">Replication Factor</label>
                  <select onChange={this.handleSelectReplicaN} defaultValue={replicaN} className="form-control input-lg" name="replication-factor" id="replication-factor" required={true}>
                    {
                      this.props.replicationFactors.map((rp) => {
                        return <option key={rp}>{rp}</option>;
                      })
                    }
                  </select>
                </div>
              </div>
              <div className="modal-footer">
                <button type="button" className="btn btn-default" data-dismiss="modal">Cancel</button>
                <button type="submit" className="btn btn-success">Create</button>
              </div>
            </div>
          </div>
        </form>
      </div>
    );
  },
});

export default CreateDatabase;
