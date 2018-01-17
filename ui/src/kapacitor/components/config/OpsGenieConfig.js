import React, {PropTypes, Component} from 'react'

import RedactedInput from './RedactedInput'
import TagInput from 'shared/components/TagInput'

class OpsGenieConfig extends Component {
  constructor(props) {
    super(props)

    const {teams, recipients} = props.config.options

    this.state = {
      currentTeams: teams || [],
      currentRecipients: recipients || [],
    }
  }

  handleSaveAlert = e => {
    e.preventDefault()

    const properties = {
      'api-key': this.apiKey.value,
      teams: this.state.currentTeams,
      recipients: this.state.currentRecipients,
    }

    this.props.onSave(properties)
  }

  handleAddTeam = team => {
    this.setState({currentTeams: this.state.currentTeams.concat(team)})
  }

  handleAddRecipient = recipient => {
    this.setState({
      currentRecipients: this.state.currentRecipients.concat(recipient),
    })
  }

  handleDeleteTeam = team => () => {
    this.setState({
      currentTeams: this.state.currentTeams.filter(t => t !== team),
    })
  }

  handleDeleteRecipient = recipient => () => {
    this.setState({
      currentRecipients: this.state.currentRecipients.filter(
        r => r !== recipient
      ),
    })
  }

  handleApiKeyRef = r => (this.apiKey = r)

  render() {
    const {options} = this.props.config
    const apiKey = options['api-key']
    const {currentTeams, currentRecipients} = this.state

    return (
      <form onSubmit={this.handleSaveAlert}>
        <div className="form-group col-xs-12">
          <label htmlFor="api-key">API Key</label>
          <RedactedInput
            defaultValue={apiKey}
            id="api-key"
            refFunc={this.handleApiKeyRef}
          />
          <label className="form-helper">
            Note: a value of <code>true</code> indicates the OpsGenie API key
            has been set
          </label>
        </div>

        <TagInput
          title="Teams"
          onAddTag={this.handleAddTeam}
          onDeleteTag={this.handleDeleteTeam}
          tags={currentTeams}
        />
        <TagInput
          title="Recipients"
          onAddTag={this.handleAddRecipient}
          onDeleteTag={this.handleDeleteRecipient}
          tags={currentRecipients}
        />

        <div className="form-group-submit col-xs-12 text-center">
          <button className="btn btn-primary" type="submit">
            Update OpsGenie Config
          </button>
        </div>
      </form>
    )
  }
}

const {array, bool, func, shape} = PropTypes

OpsGenieConfig.propTypes = {
  config: shape({
    options: shape({
      'api-key': bool,
      teams: array,
      recipients: array,
    }).isRequired,
  }).isRequired,
  onSave: func.isRequired,
}

export default OpsGenieConfig
