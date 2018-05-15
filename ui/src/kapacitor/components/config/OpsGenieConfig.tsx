import _ from 'lodash'
import React, {PureComponent, ChangeEvent} from 'react'

import RedactedInput from 'src/kapacitor/components/config/RedactedInput'
import TagInput from 'src/shared/components/TagInput'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {OpsGenieProperties} from 'src/types/kapacitor'

interface Config {
  options: {
    'api-key': boolean
    teams: string[]
    recipients: string[]
    enabled: boolean
  }
}

interface Item {
  name?: string
}

interface Props {
  config: Config
  onSave: (properties: OpsGenieProperties) => void
  onTest: (event: React.MouseEvent<HTMLButtonElement>) => void
  enabled: boolean
}

interface State {
  currentTeams: string[]
  currentRecipients: string[]
  testEnabled: boolean
  enabled: boolean
}

@ErrorHandling
class OpsGenieConfig extends PureComponent<Props, State> {
  private apiKey: HTMLInputElement

  constructor(props) {
    super(props)

    const {teams, recipients} = props.config.options

    this.state = {
      currentTeams: teams || [],
      currentRecipients: recipients || [],
      testEnabled: this.props.enabled,
      enabled: _.get(this.props, 'config.options.enabled', false),
    }
  }

  public render() {
    const {options} = this.props.config
    const apiKey = options['api-key']
    const {testEnabled, enabled} = this.state

    return (
      <form onSubmit={this.handleSubmit}>
        <div className="form-group col-xs-12">
          <label htmlFor="api-key">API Key</label>
          <RedactedInput
            defaultValue={apiKey}
            id="api-key"
            refFunc={this.handleApiKeyRef}
            disableTest={this.disableTest}
            isFormEditing={!testEnabled}
          />
        </div>

        <TagInput
          title="Teams"
          onAddTag={this.handleAddTeam}
          onDeleteTag={this.handleDeleteTeam}
          tags={this.currentTeamsForTags}
          disableTest={this.disableTest}
        />
        <TagInput
          title="Recipients"
          onAddTag={this.handleAddRecipient}
          onDeleteTag={this.handleDeleteRecipient}
          tags={this.currentRecipientsForTags}
          disableTest={this.disableTest}
        />

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
      </form>
    )
  }

  private handleEnabledChange = (e: ChangeEvent<HTMLInputElement>) => {
    this.setState({enabled: e.target.checked})
    this.disableTest()
  }

  private get currentTeamsForTags(): Item[] {
    const {currentTeams} = this.state
    return currentTeams.map(team => ({name: team}))
  }

  private get currentRecipientsForTags(): Item[] {
    const {currentRecipients} = this.state
    return currentRecipients.map(recipient => ({name: recipient}))
  }

  private handleSubmit = async e => {
    e.preventDefault()

    const properties: OpsGenieProperties = {
      'api-key': this.apiKey.value,
      teams: this.state.currentTeams,
      recipients: this.state.currentRecipients,
      enabled: this.state.enabled,
    }

    const success = await this.props.onSave(properties)
    if (success) {
      this.setState({testEnabled: true})
    }
  }

  private disableTest = () => {
    this.setState({testEnabled: false})
  }

  private handleAddTeam = team => {
    this.setState({currentTeams: this.state.currentTeams.concat(team)})
  }

  private handleAddRecipient = recipient => {
    this.setState({
      currentRecipients: this.state.currentRecipients.concat(recipient),
    })
  }

  private handleDeleteTeam = team => {
    this.setState({
      currentTeams: this.state.currentTeams.filter(t => t !== team.name),
      testEnabled: false,
    })
  }

  private handleDeleteRecipient = recipient => {
    this.setState({
      currentRecipients: this.state.currentRecipients.filter(
        r => r !== recipient.name
      ),
      testEnabled: false,
    })
  }

  private handleApiKeyRef = r => (this.apiKey = r)
}

export default OpsGenieConfig
