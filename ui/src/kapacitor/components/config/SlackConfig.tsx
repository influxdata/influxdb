import _ from 'lodash'
import React, {PureComponent, ChangeEvent} from 'react'
import RedactedInput from 'src/kapacitor/components/config/RedactedInput'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Properties {
  channel: string
  url: string
}

interface Config {
  options: {
    url: boolean
    channel: string
    enabled: boolean
    workspace: string
  }
}

interface Props {
  config: Config
  onSave: (properties: Properties, isNewConfigInSection: boolean) => void
  onTest: (event: React.MouseEvent<HTMLButtonElement>) => void
  enabled: boolean
  isNewConfig: boolean
}

interface State {
  testEnabled: boolean
  enabled: boolean
}

@ErrorHandling
class SlackConfig extends PureComponent<Props, State> {
  private url: HTMLInputElement
  private channel: HTMLInputElement
  private workspace: HTMLInputElement

  constructor(props) {
    super(props)
    this.state = {
      testEnabled: this.props.enabled,
      enabled: _.get(this.props, 'config.options.enabled', false),
    }
  }

  public render() {
    const {url, channel, workspace} = this.props.config.options
    const {testEnabled, enabled} = this.state

    return (
      <form onSubmit={this.handleSubmit}>
        <div className="form-group col-xs-12">
          <label htmlFor="nickname">Nickname this Configuration</label>
          <input
            className="form-control"
            id="nickname"
            type="text"
            placeholder="Optional unless multiple Slack configurations exist"
            ref={r => (this.workspace = r)}
            defaultValue={workspace}
            onChange={this.disableTest}
          />
        </div>
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
            isFormEditing={!testEnabled}
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

        <div className="form-group col-xs-12">
          <div className="form-control-static">
            <input
              type="checkbox"
              id="disabled"
              checked={enabled}
              onChange={this.handleEnabledChange}
            />
            <label htmlFor="disabled">Configuration Enabled</label>
          </div>
        </div>

        <div className="form-group form-group-submit col-xs-12 text-center">
          <button
            className="btn btn-primary"
            type="submit"
            disabled={this.state.testEnabled}
          >
            <span className="icon checkmark" />
            Save Changes
          </button>
          <button
            className="btn btn-primary"
            disabled={!this.state.testEnabled || !enabled}
            onClick={this.props.onTest}
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

  private handleEnabledChange = (e: ChangeEvent<HTMLInputElement>) => {
    this.setState({enabled: e.target.checked})
    this.disableTest()
  }

  private handleSubmit = async e => {
    const {isNewConfig} = this.props
    e.preventDefault()
    const properties = {
      url: this.url.value,
      channel: this.channel.value,
      enabled: this.state.enabled,
      workspace: this.workspace.value,
    }
    const success = await this.props.onSave(properties, isNewConfig)
    if (success) {
      this.setState({testEnabled: true})
    }
  }

  private disableTest = () => {
    this.setState({testEnabled: false})
  }

  private handleUrlRef = r => (this.url = r)
}

export default SlackConfig
