import React, {PropTypes} from 'react';

const PagerDutyConfig = React.createClass({
  propTypes: {
    config: PropTypes.shape({
      options: PropTypes.shape({
        'service-key': PropTypes.bool.isRequired,
        url: PropTypes.string.isRequired,
      }).isRequired,
    }).isRequired,
    onSave: PropTypes.func.isRequired,
  },

  handleSaveAlert(e) {
    e.preventDefault();

    const properties = {
      serviceKey: this.serviceKey.value,
      url: this.url.value,
    };

    this.props.onSave(properties);
  },

  render() {
    const {options} = this.props.config;
    const {url} = options;
    const serviceKey = options['service-key'];

    return (
      <div>
        <h4 className="text-center">PagerDuty Alert</h4>
        <br/>
        <p>You can have alerts sent to PagerDuty by entering info below.</p>
        <form onSubmit={this.handleSaveAlert}>
          <div className="form-group col-xs-12">
            <label htmlFor="service-key">Service Key</label>
            <input className="form-control" id="service-key" type="text" ref={(r) => this.serviceKey = r} defaultValue={serviceKey || ''}></input>
            <label className="form-helper">Note: a value of <code>true</code> indicates the PagerDuty service key has been set</label>
          </div>

          <div className="form-group col-xs-12">
            <label htmlFor="url">PagerDuty URL</label>
            <input className="form-control" id="url" type="text" ref={(r) => this.url = r} defaultValue={url || ''}></input>
          </div>

          <div className="form-group form-group-submit col-xs-12 col-sm-6 col-sm-offset-3">
            <button className="btn btn-block btn-primary" type="submit">Save</button>
          </div>
        </form>
      </div>
    );
  },
});

export default PagerDutyConfig;
