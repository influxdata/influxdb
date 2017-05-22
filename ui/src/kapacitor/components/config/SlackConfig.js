import React, {PropTypes} from 'react'

import RedactedInput from './RedactedInput'

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
      <form onSubmit={this.handleSaveAlert}>
        <div className="form-group col-xs-12">
          <label htmlFor="slack-url">
            Slack Webhook URL (
            <a href="https://api.slack.com/incoming-webhooks" target="_">
              see more on Slack webhooks
            </a>
            )
          </label>
          <RedactedInput
            defaultValue={url}
            id="url"
            refFunc={r => (this.url = r)}
          />
        </div>

        <div className="form-group col-xs-12">
          <label htmlFor="slack-channel">Slack Channel (optional)</label>
          <input
            className="form-control"
            id="slack-channel"
            type="text"
            placeholder="#alerts"
            ref={r => (this.channel = r)}
            defaultValue={channel || ''}
          />
        </div>

        <div className="form-group-submit col-xs-12 text-center">
          <button className="btn btn-primary" type="submit">
            Update Slack Config
          </button>
        </div>
      </form>
    )
  },
})

export default SlackConfig
