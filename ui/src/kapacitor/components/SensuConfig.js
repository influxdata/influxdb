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
      <div className="panel-body">
        <h4 className="text-center">Sensu Alert</h4>
        <br/>
        <form onSubmit={this.handleSaveAlert}>
          <div className="row">
            <div className="col-xs-7 col-sm-8 col-sm-offset-2">
              <p>
                Have alerts sent to Sensu
              </p>

              <div className="form-group">
                <label htmlFor="source">Source</label>
                <input className="form-control" id="source" type="text" ref={(r) => this.source = r} defaultValue={source || ''}></input>
              </div>

              <div className="form-group">
                <label htmlFor="address">Address</label>
                <input className="form-control" id="address" type="text" ref={(r) => this.addr = r} defaultValue={addr || ''}></input>
              </div>
            </div>
          </div>

          <hr />
          <div className="row">
            <div className="form-group col-xs-5 col-sm-3 col-sm-offset-2">
              <button className="btn btn-block btn-primary" type="submit">Save</button>
            </div>
          </div>
        </form>
      </div>
    );
  },
});

export default SensuConfig;
