import React, {PropTypes} from 'react';

const SensuConfig = React.createClass({
  propTypes: {
    config: PropTypes.shape({
      options: PropTypes.shape({
        source: PropTypes.string.isRequired,
        addr: PropTypes.string.isRequired,
      }).isRequired,
    }).isRequired,
    onSave: PropTypes.func.isRequired,
  },

  handleSaveAlert(e) {
    e.preventDefault();

    const properties = {
      source: this.source.value,
      addr: this.addr.value,
    };

    this.props.onSave(properties);
  },

  render() {
    const {source, addr} = this.props.config.options;

    return (
      <div>
        <h4 className="text-center no-user-select">Sensu Alert</h4>
        <br/>
        <p className="no-user-select">Have alerts sent to Sensu.</p>
        <form onSubmit={this.handleSaveAlert}>
          <div className="form-group col-xs-12 col-md-6">
            <label htmlFor="source">Source</label>
            <input className="form-control" id="source" type="text" ref={(r) => this.source = r} defaultValue={source || ''}></input>
          </div>

          <div className="form-group col-xs-12 col-md-6">
            <label htmlFor="address">Address</label>
            <input className="form-control" id="address" type="text" ref={(r) => this.addr = r} defaultValue={addr || ''}></input>
          </div>

          <div className="form-group form-group-submit col-xs-12 col-sm-6 col-sm-offset-3">
            <button className="btn btn-block btn-primary" type="submit">Save</button>
          </div>
        </form>
      </div>
    );
  },
});

export default SensuConfig;
