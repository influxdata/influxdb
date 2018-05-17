import _ from 'lodash'
import React, {PureComponent, ChangeEvent, MouseEvent} from 'react'
import RedactedInput from 'src/kapacitor/components/config/RedactedInput'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {SlackProperties} from 'src/types/kapacitor'

interface Options {
  url: boolean
  channel: string
  workspace: string
}

interface Config {
  options: Options
}

interface Props {
  config: Config
  onSave: (
    properties: SlackProperties,
    isNewConfigInSection: boolean,
    specificConfig: string
  ) => void
  onTest: (
    e: MouseEvent<HTMLButtonElement>,
    specificConfigOptions: Partial<SlackProperties>
  ) => void
  onDelete: (specificConfig: string, workspaceID: string) => void
  enabled: boolean
  isNewConfig: boolean
  workspaceID: string
  isDefaultConfig: boolean
}

interface State {
  testEnabled: boolean
  enabled: boolean
  workspace: string
}

@ErrorHandling
class SlackConfig extends PureComponent<Props, State> {
  private url: HTMLInputElement
  private channel: HTMLInputElement

  constructor(props) {
    super(props)
    this.state = {
      testEnabled: this.props.enabled,
      enabled: _.get(this.props, 'config.options.enabled', false),
      workspace: _.get(this.props, 'config.options.workspace') || '',
    }
  }

  public render() {
    const {url, channel} = this.options
    const {testEnabled, enabled, workspace} = this.state

    return (
      <form onSubmit={this.handleSubmit}>
        <div className="form-group col-xs-12">
          <label htmlFor={`${this.workspaceID}-nickname`}>
            Nickname this Configuration
          </label>
          <input
            className="form-control"
            id={`${this.workspaceID}-nickname`}
            type="text"
            placeholder={this.nicknamePlaceholder}
            value={workspace}
            onChange={this.handleWorkspaceChange}
            disabled={!this.isNewConfig && !testEnabled}
          />
        </div>
        <div className="form-group col-xs-12">
          <label htmlFor={`${this.workspaceID}-url`}>
            Slack Webhook URL (
            <a href="https://api.slack.com/incoming-webhooks" target="_">
              see more on Slack webhooks
            </a>
            )
          </label>
          <RedactedInput
            defaultValue={url}
            id={`${this.workspaceID}-url`}
            refFunc={this.handleUrlRef}
            disableTest={this.disableTest}
            isFormEditing={!testEnabled}
          />
        </div>

        <div className="form-group col-xs-12">
          <label htmlFor={`${this.workspaceID}-slack-channel`}>
            Slack Channel (optional)
          </label>
          <input
            className="form-control"
            id={`${this.workspaceID}-slack-channel`}
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
              id={`${this.workspaceID}-disabled`}
              checked={enabled}
              onChange={this.handleEnabledChange}
            />
            <label htmlFor={`${this.workspaceID}-disabled`}>
              Configuration Enabled
            </label>
          </div>
        </div>

        <div className="form-group form-group-submit col-xs-12 text-center">
          <button
            className="btn btn-primary"
            type="submit"
            disabled={testEnabled || this.isWorkspaceEmpty}
          >
            <span className="icon checkmark" />
            Save Changes
          </button>
          <button
            className="btn btn-primary"
            disabled={this.isTestDisabled}
            onClick={this.handleTest}
          >
            <span className="icon pulse-c" />
            Send Test Alert
          </button>
          {this.deleteButton}
          <hr />
        </div>
      </form>
    )
  }

  private handleWorkspaceChange = (e: ChangeEvent<HTMLInputElement>) => {
    this.setState({workspace: e.target.value})
  }

  private get isNewConfig(): boolean {
    const {isNewConfig} = this.props

    return isNewConfig
  }

  private get options(): Options {
    const {
      config: {options},
    } = this.props

    return options
  }

  private get deleteButton(): JSX.Element {
    if (this.isDefaultConfig) {
      return null
    }

    return (
      <button className="btn btn-danger" onClick={this.handleDelete}>
        <span className="icon trash" />
        Delete
      </button>
    )
  }

  private get nicknamePlaceholder(): string {
    if (this.isDefaultConfig) {
      return 'Only for additional Configurations'
    }
    return 'Must be different from previous Configurations'
  }

  private get isDefaultConfig(): boolean {
    return this.props.isDefaultConfig
  }

  private get workspaceID(): string {
    return this.props.workspaceID
  }

  private get isWorkspaceEmpty(): boolean {
    const {workspace} = this.state
    return workspace === '' && !this.isDefaultConfig
  }

  private get isTestDisabled(): boolean {
    const {testEnabled, enabled} = this.state
    return !testEnabled || !enabled || this.isWorkspaceEmpty
  }

  private handleTest = (e: MouseEvent<HTMLButtonElement>) => {
    const {
      onTest,
      config: {
        options: {workspace, channel},
      },
    } = this.props
    onTest(e, {workspace, channel})
  }

  private handleEnabledChange = (e: ChangeEvent<HTMLInputElement>) => {
    this.setState({enabled: e.target.checked})
    this.disableTest()
  }

  private handleSubmit = async e => {
    e.preventDefault()

    const {workspace} = this.state
    const {isNewConfig} = this.props

    const properties: SlackProperties = {
      url: this.url.value,
      channel: this.channel.value,
      enabled: this.state.enabled,
    }

    if (isNewConfig) {
      properties.workspace = workspace
    }

    const success = await this.props.onSave(properties, isNewConfig, workspace)
    if (success) {
      this.setState({testEnabled: true})
    }
  }

  private handleDelete = async e => {
    e.preventDefault()
    const {workspace} = this.state
    await this.props.onDelete(workspace, this.workspaceID)
  }

  private disableTest = () => {
    this.setState({testEnabled: false})
  }

  private handleUrlRef = r => (this.url = r)
}

export default SlackConfig
