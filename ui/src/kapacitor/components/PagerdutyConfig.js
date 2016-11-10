import React, {PropTypes} from 'react';

const PagerDutyConfig = React.createClass({
  propTypes: {
    config: PropTypes.shape({
      options: PropTypes.shape({
        global: PropTypes.bool.isRequired,
        'service-key': PropTypes.bool.isRequired,
        url: PropTypes.string.isRequired,
      }).isRequired,
    }).isRequired,
    onSave: PropTypes.func.isRequired,
  },

  handleSaveAlert(e) {
    e.preventDefault();

    const properties = {
      global: this.global.checked,
      serviceKey: this.serviceKey.value,
      url: this.url.value,
    };

    this.props.onSave(properties);
  },

  render() {
    const {options} = this.props.config;
    const {global, url} = options;
    const serviceKey = options['service-key'];

    return (
      <div className="panel-body">
        <h4 className="text-center">PagerDuty Alert</h4>
        <br/>
        <form onSubmit={this.handleSaveAlert}>
          <div className="row">
            <div className="col-xs-7 col-sm-8 col-sm-offset-2">
              <p>
                You can have alerts sent to PagerDuty by entering info below.
              </p>

              <div className="form-group">
                <label htmlFor="service-key">Service Key</label>
                <input className="form-control" id="service-key" type="text" ref={(r) => this.serviceKey = r} defaultValue={serviceKey || ''}></input>
                <span>Note: a value of <code>true</code> indicates the PagerDuty service key has been set</span>
              </div>

              <div className="form-group">
                <label htmlFor="url">PagerDuty URL</label>
                <input className="form-control" id="url" type="text" ref={(r) => this.url = r} defaultValue={url || ''}></input>
              </div>

              <div className="form-group col-xs-12">
                <div className="checkbox">
                  <label>
                    <input type="checkbox" defaultChecked={global} ref={(r) => this.global = r} />
                    Send all alerts without marking them explicitly in TICKscript
                  </label>
                </div>
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

export default PagerDutyConfig;
