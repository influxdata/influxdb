import React, {PureComponent} from 'react'

import RedactedInput from './RedactedInput'
import TagInput from 'src/shared/components/TagInput'
import {Input} from 'src/types/kapacitor'

interface Properties {
  'api-key': string
  teams: string[]
  recipients: string[]
}

interface Config {
  options: {
    'api-key': boolean
    teams: string[]
    recipients: string[]
  }
}

interface Props {
  config: Config
  onSave: (properties: Properties) => void
  onTest: (event: React.MouseEvent<HTMLButtonElement>) => void
  enabled: boolean
}

interface State {
  currentTeams: string[]
  currentRecipients: string[]
  testEnabled: boolean
}

class OpsGenieConfig extends PureComponent<Props, State> {
  private apiKey: Input

  constructor(props) {
    super(props)

    const {teams, recipients} = props.config.options

    this.state = {
      currentTeams: teams || [],
      currentRecipients: recipients || [],
      testEnabled: this.props.enabled,
    }
  }

  public render() {
    const {options} = this.props.config
    const apiKey = options['api-key']
    const {currentTeams, currentRecipients} = this.state

    return (
      <form onSubmit={this.handleSubmit}>
        <div className="form-group col-xs-12">
          <label htmlFor="api-key">API Key</label>
          <RedactedInput
            defaultValue={apiKey}
            id="api-key"
            refFunc={this.handleApiKeyRef}
            disableTest={this.disableTest}
          />
        </div>

        <TagInput
          title="Teams"
          onAddTag={this.handleAddTeam}
          onDeleteTag={this.handleDeleteTeam}
          tags={currentTeams}
          disableTest={this.disableTest}
        />
        <TagInput
          title="Recipients"
          onAddTag={this.handleAddRecipient}
          onDeleteTag={this.handleDeleteRecipient}
          tags={currentRecipients}
          disableTest={this.disableTest}
        />

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
            disabled={!this.state.testEnabled}
            onClick={this.props.onTest}
          >
            <span className="icon pulse-c" />
            Send Test Alert
          </button>
        </div>
      </form>
    )
  }

  private handleSubmit = async e => {
    e.preventDefault()

    const properties = {
      'api-key': this.apiKey.value,
      teams: this.state.currentTeams,
      recipients: this.state.currentRecipients,
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
      currentTeams: this.state.currentTeams.filter(t => t !== team),
    })
  }

  private handleDeleteRecipient = recipient => {
    this.setState({
      currentRecipients: this.state.currentRecipients.filter(
        r => r !== recipient
      ),
    })
  }

  private handleApiKeyRef = r => (this.apiKey = r)
}

export default OpsGenieConfig
