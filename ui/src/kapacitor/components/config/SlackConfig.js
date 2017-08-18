import React, {PropTypes, Component} from 'react'

import RedactedInput from './RedactedInput'

class SlackConfig extends Component {
  constructor(props) {
    super(props)
    this.state = {
      testEnabled: !!this.props.config.options.url,
    }
  }

  componentWillReceiveProps(nextProps) {
    this.setState({
      testEnabled: !!nextProps.config.options.url,
    })
  }

  handleSaveAlert = e => {
    e.preventDefault()

    const properties = {
      url: this.url.value,
      channel: this.channel.value,
    }

    this.props.onSave(properties)
  }

  handleUrlRef = r => (this.url = r)

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
            refFunc={this.handleUrlRef}
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
}

export default SlackConfig
