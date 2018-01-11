import React, {PropTypes, Component} from 'react'

import RedactedInput from './RedactedInput'

class SlackConfig extends Component {
  constructor(props) {
    super(props)
    this.state = {
      testEnabled: this.props.enabled,
    }
  }

  handleSubmit = e => {
    e.preventDefault()
    if (this.state.testEnabled) {
      this.props.onTest()
      return
    }
    const properties = {
      url: this.url.value,
      channel: this.channel.value,
    }
    this.props.onSave(properties)
    this.setState({testEnabled: true})
  }
  disableTest = () => {
    this.setState({testEnabled: false})
  }

  handleUrlRef = r => (this.url = r)

  render() {
    const {url, channel} = this.props.config.options

    return (
      <form onSubmit={this.handleSubmit}>
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
            refFunc={this.handleUrlRef}
            disableTest={this.disableTest}
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
            onChange={this.disableTest}
          />
        </div>

        <div className="form-group-submit col-xs-12 text-center">
          <button
            className="btn btn-primary"
            type="submit"
            disabled={this.state.testEnabled}
            onClick={this.enableTest}
          >
            <span className="icon checkmark" />
            Save Changes
          </button>
          <button
            className="btn btn-primary"
            disabled={!this.state.testEnabled}
            onClick={this.enableTest}
          >
            <span className="icon pulse-c" />
            Send Test Alert
          </button>
        </div>
        <br />
        <br />
      </form>
    )
  }
}

const {bool, func, shape, string} = PropTypes

SlackConfig.propTypes = {
  config: shape({
    options: shape({
      url: bool.isRequired,
      channel: string.isRequired,
    }).isRequired,
  }).isRequired,
  onSave: func.isRequired,
  onTest: func,
  enabled: bool.isRequired,
}

export default SlackConfig
