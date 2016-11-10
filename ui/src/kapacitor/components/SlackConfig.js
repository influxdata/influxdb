import React, {PropTypes} from 'react';

const SlackConfig = React.createClass({
  propTypes: {
    config: PropTypes.shape({
      options: PropTypes.shape({
        url: PropTypes.bool.isRequired,
        channel: PropTypes.string.isRequired,
      }).isRequired,
    }).isRequired,
    onSave: PropTypes.func.isRequired,
    onTest: PropTypes.func.isRequired,
  },

  getInitialState() {
    return {
      testEnabled: !!this.props.config.options.url,
    };
  },

  componentWillReceiveProps(nextProps) {
    this.setState({
      testEnabled: !!nextProps.config.options.url,
    });
  },

  handleSaveAlert(e) {
    e.preventDefault();

    const properties = {
      url: this.url.value,
      channel: this.channel.value,
    };

    this.props.onSave(properties);
  },

  handleTest(e) {
    e.preventDefault();
    this.props.onTest({
      url: this.url.value,
      channel: this.channel.value,
    });
  },

  render() {
    const {url, channel} = this.props.config.options;

    return (
      <div className="panel-body">
        <h4 className="text-center">Slack Alert</h4>
        <br/>
        <form onSubmit={this.handleSaveAlert}>
          <div className="row">
            <div className="col-xs-7 col-sm-8 col-sm-offset-2">
              <p>
                Post alerts to a Slack channel.
              </p>

              <div className="form-group">
                <label htmlFor="slack-url">Slack Webhook URL (<a href="https://api.slack.com/incoming-webhooks" target="_">see more on Slack webhooks</a>)</label>
                <input className="form-control" id="slack-url" type="text" ref={(r) => this.url = r} defaultValue={url || ''}></input>
                <span>Note: a value of <code>true</code> indicates that the Slack channel has been set</span>
              </div>

              <div className="form-group">
                <label htmlFor="slack-channel">Slack Channel (optional)</label>
                <input className="form-control" id="slack-channel" type="text" placeholder="#alerts" ref={(r) => this.channel = r} defaultValue={channel || ''}></input>
              </div>
            </div>

            <div className="form-group col-xs-5 col-sm-3 col-sm-offset-2">
              <button className="btn btn-block btn-primary" type="submit">Save</button>
            </div>
          </div>

          <hr />

          <div className="row">
            <div className="form-group col-xs-5 col-sm-3 col-sm-offset-2">
              <a className="btn btn-warning" onClick={this.handleTest} disabled={!this.state.testEnabled}>Send Test Message</a>
            </div>
          </div>
        </form>
      </div>
    );
  },
});

export default SlackConfig;
