import React, {Component, PropTypes} from 'react'

class Organization extends Component {
  constructor(props) {
    super(props)

    this.state = {
      organizationName: 'Untitled Organization',
    }
  }

  handleKeyDown = e => {
    const {onCancelCreateOrganization} = this.props

    if (e.key === 'Escape') {
      onCancelCreateOrganization()
      this.inputRef.blur()
    }
    if (e.key === 'Enter') {
      this.handleClickSave()
      this.inputRef.blur()
    }
  }

  handleInputChange = e => {
    this.setState({organizationName: e.target.value.trim()})
  }

  handleInputFocus = e => {
    e.target.select()
  }

  handleClickSave = () => {
    const {onCancelCreateOrganization, onCreateOrganization} = this.props
    const {organizationName} = this.state

    onCreateOrganization(organizationName)
    onCancelCreateOrganization()
  }

  render() {
    const {organizationName} = this.state
    const {onCancelCreateOrganization} = this.props

    const isSaveDisabled = organizationName === null || organizationName === ''

    return (
      <div className="manage-orgs-form--org manage-orgs-form--new-org">
        <div className="manage-orgs-form--id">&mdash;</div>
        <input
          type="text"
          className="form-control input-sm manage-orgs-form--input"
          value={organizationName}
          onKeyDown={this.handleKeyDown}
          onChange={this.handleInputChange}
          onFocus={this.handleInputFocus}
          placeholder="Name this Organization..."
          autoFocus={true}
          ref={r => (this.inputRef = r)}
        />
        <button
          className="btn btn-sm btn-square btn-default"
          onClick={onCancelCreateOrganization}
        >
          <span className="icon remove" />
        </button>
        <button
          className="btn btn-sm btn-square btn-success"
          onClick={this.handleClickSave}
          disabled={isSaveDisabled}
        >
          <span className="icon checkmark" />
        </button>
      </div>
    )
  }
}

const {func} = PropTypes

Organization.propTypes = {
  onCreateOrganization: func.isRequired,
  onCancelCreateOrganization: func.isRequired,
}

export default Organization
