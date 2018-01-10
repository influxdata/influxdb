import React, {PropTypes, Component} from 'react'
import _ from 'lodash'

import RedactedInput from './RedactedInput'

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

const {array, arrayOf, bool, func, shape, string} = PropTypes

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

class TagInput extends Component {
  constructor(props) {
    super(props)
  }

  handleAddTag = e => {
    if (e.key === 'Enter') {
      e.preventDefault()
      const newItem = e.target.value.trim()
      const {tags, onAddTag} = this.props
      if (!this.shouldAddToList(newItem, tags)) {
        return
      }

      this.input.value = ''
      onAddTag(newItem)
    }
  }

  shouldAddToList(item, tags) {
    return !_.isEmpty(item) && !tags.find(l => l === item)
  }

  render() {
    const {title, tags, onDeleteTag} = this.props

    return (
      <div className="form-group col-xs-12">
        <label htmlFor={title}>
          {title}
        </label>
        <input
          placeholder={`Type and hit 'Enter' to add to list of ${title}`}
          autoComplete="off"
          className="form-control"
          id={title}
          type="text"
          ref={r => (this.input = r)}
          onKeyDown={this.handleAddTag}
        />
        <Tags tags={tags} onDeleteTag={onDeleteTag} />
      </div>
    )
  }
}

TagInput.propTypes = {
  onAddTag: func.isRequired,
  onDeleteTag: func.isRequired,
  tags: arrayOf(string).isRequired,
  title: string.isRequired,
}

const Tags = ({tags, onDeleteTag}) =>
  <div className="input-tag-list">
    {tags.map(item => {
      return <Tag key={item} item={item} onDelete={onDeleteTag} />
    })}
  </div>

Tags.propTypes = {
  tags: arrayOf(string),
  onDeleteTag: func,
}

const Tag = ({item, onDelete}) =>
  <span key={item} className="input-tag-item">
    <span>
      {item}
    </span>
    <span className="icon remove" onClick={onDelete(item)} />
  </span>

Tag.propTypes = {
  item: string,
  onDelete: func,
}

export default OpsGenieConfig
