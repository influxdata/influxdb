import _ from 'lodash'
import React, {PureComponent, ChangeEvent} from 'react'
import RedactedInput from 'src/kapacitor/components/config/RedactedInput'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Properties {
  channel: string
  url: string
  workspace?: string
  enabled: boolean
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
  onSave: (
    properties: Properties,
    isNewConfigInSection: boolean,
    specificConfig: string
  ) => void
  onTest: (event: React.MouseEvent<HTMLButtonElement>) => void
  onDelete: (specificConfig: string) => void
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
    const {
      config: {
        options: {url, channel, workspace},
      },
      isNewConfig,
    } = this.props
    const {testEnabled, enabled} = this.state
    const workspaceID = workspace || 'default'

    const isNickNameEnabled = isNewConfig && !testEnabled

    return (
      <form onSubmit={this.handleSubmit}>
        <div className="form-group col-xs-12">
          <label htmlFor={`${workspaceID}-nickname`}>
            Nickname this Configuration
          </label>
          <input
            className="form-control"
            id={`${workspaceID}-nickname`}
            type="text"
            placeholder={this.nicknamePlaceholder}
            ref={r => (this.workspace = r)}
            defaultValue={workspace || ''}
            onChange={this.disableTest}
            disabled={!isNickNameEnabled}
          />
        </div>
        <div className="form-group col-xs-12">
          <label htmlFor={`${workspaceID}-url`}>
            Slack Webhook URL (
            <a href="https://api.slack.com/incoming-webhooks" target="_">
              see more on Slack webhooks
            </a>
            )
          </label>
          <RedactedInput
            defaultValue={url}
            id={`${workspaceID}-url`}
            refFunc={this.handleUrlRef}
            disableTest={this.disableTest}
            isFormEditing={!testEnabled}
          />
        </div>

        <div className="form-group col-xs-12">
          <label htmlFor={`${workspaceID}-slack-channel`}>
            Slack Channel (optional)
          </label>
          <input
            className="form-control"
            id={`${workspaceID}-slack-channel`}
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
              id={`${workspaceID}-disabled`}
              checked={enabled}
              onChange={this.handleEnabledChange}
            />
            <label htmlFor={`${workspaceID}-disabled`}>
              Configuration Enabled
            </label>
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
          {!this.isDefaultConfig && (
            <button className="btn btn-danger" onClick={this.handleDelete}>
              <span className="icon trash" />
              Delete
            </button>
          )}
        </div>
        <br />
        <br />
      </form>
    )
  }

  private get nicknamePlaceholder(): string {
    if (this.isDefaultConfig) {
      return 'Only for additional Configurations'
    }
    return 'Must be different from previous Configurations'
  }

  private get isDefaultConfig(): boolean {
    const {
      config: {
        options: {workspace},
      },
      isNewConfig,
    } = this.props
    return workspace === '' && !isNewConfig
  }

  private handleEnabledChange = (e: ChangeEvent<HTMLInputElement>) => {
    this.setState({enabled: e.target.checked})
    this.disableTest()
  }

  private handleSubmit = async e => {
    const {isNewConfig} = this.props
    e.preventDefault()
    const properties: Properties = {
      url: this.url.value,
      channel: this.channel.value,
      enabled: this.state.enabled,
    }
    if (isNewConfig) {
      properties.workspace = this.workspace.value
    }
    const success = await this.props.onSave(
      properties,
      isNewConfig,
      this.workspace.value
    )
    if (success) {
      this.setState({testEnabled: true})
    }
  }

  private handleDelete = async e => {
    e.preventDefault()
    await this.props.onDelete(this.workspace.value)
  }

  private disableTest = () => {
    this.setState({testEnabled: false})
  }

  private handleUrlRef = r => (this.url = r)
}

export default SlackConfig
