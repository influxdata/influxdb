import React, {PropTypes} from 'react';

const VictorOpsConfig = React.createClass({
  propTypes: {
    config: PropTypes.shape({
      options: PropTypes.shape({
        'api-key': PropTypes.bool,
        'routing-key': PropTypes.string,
        url: PropTypes.string,
      }).isRequired,
    }).isRequired,
    onSave: PropTypes.func.isRequired,
  },

  handleSaveAlert(e) {
    e.preventDefault();

    const properties = {
      'api-key': this.apiKey.value,
      'routing-key': this.routingKey.value,
      url: this.url.value,
    };

    this.props.onSave(properties);
  },

  render() {
    const {options} = this.props.config;
    const apiKey = options['api-key'];
    const routingKey = options['routing-key'];
    const {url} = options;

    return (
      <div className="panel-body">
        <h4 className="text-center">VictorOps Alert</h4>
        <br/>
        <form onSubmit={this.handleSaveAlert}>
          <div className="row">
            <div className="col-xs-7 col-sm-8 col-sm-offset-2">
              <p>
                Have alerts sent to VictorOps
              </p>

              <div className="form-group">
                <label htmlFor="api-key">API Key</label>
                <input className="form-control" id="api-key" type="text" ref={(r) => this.apiKey = r} defaultValue={apiKey || ''}></input>
                <span>Note: a value of <code>true</code> indicates the VictorOps API key has been set</span>
              </div>

              <div className="form-group">
                <label htmlFor="routing-key">Routing Key</label>
                <input className="form-control" id="routing-key" type="text" ref={(r) => this.routingKey = r} defaultValue={routingKey || ''}></input>
              </div>

              <div className="form-group">
                <label htmlFor="url">VictorOps URL</label>
                <input className="form-control" id="url" type="text" ref={(r) => this.url = r} defaultValue={url || ''}></input>
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

export default VictorOpsConfig;
