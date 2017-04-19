import React, {PropTypes} from 'react'

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
    }
  },

  componentWillReceiveProps(nextProps) {
    this.setState({
      testEnabled: !!nextProps.config.options.url,
    })
  },

  handleSaveAlert(e) {
    e.preventDefault()

    const properties = {
      url: this.url.value,
      channel: this.channel.value,
    }

    this.props.onSave(properties)
  },

  handleTest(e) {
    e.preventDefault()
    this.props.onTest({
      url: this.url.value,
      channel: this.channel.value,
    })
  },

  render() {
    const {url, channel} = this.props.config.options

    return (
      <div className="panel panel-info col-xs-12">
        <div className="panel-heading u-flex u-ai-center u-jc-space-between">
          <h4 className="panel-title text-center">Slack Alert</h4>
        </div>
        <br/>
        <p className="no-user-select">Post alerts to a Slack channel.</p>
        <form onSubmit={this.handleSaveAlert}>
          <div className="form-group col-xs-12">
            <label htmlFor="slack-url">Slack Webhook URL (<a href="https://api.slack.com/incoming-webhooks" target="_">see more on Slack webhooks</a>)</label>
            <input className="form-control" id="slack-url" type="text" ref={(r) => this.url = r} defaultValue={url || ''}></input>
            <label className="form-helper">Note: a value of <code>true</code> indicates that the Slack channel has been set</label>
          </div>

          <div className="form-group col-xs-12">
            <label htmlFor="slack-channel">Slack Channel (optional)</label>
            <input className="form-control" id="slack-channel" type="text" placeholder="#alerts" ref={(r) => this.channel = r} defaultValue={channel || ''}></input>
          </div>

          <div className="form-group form-group-submit col-xs-12 text-center">
            <a className="btn btn-warning" onClick={this.handleTest} disabled={!this.state.testEnabled}>Send Test Message</a>
            <button className="btn btn-primary" type="submit">Save</button>
          </div>
        </form>
      </div>
    )
  },
})

export default SlackConfig
