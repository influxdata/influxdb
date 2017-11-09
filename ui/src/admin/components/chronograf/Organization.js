import React, {Component, PropTypes} from 'react'

import ConfirmButtons from 'shared/components/ConfirmButtons'

class Organization extends Component {
  constructor(props) {
    super(props)

    this.state = {
      reset: false,
      isEditing: false,
      isDeleting: false,
      workingName: this.props.organization.name,
    }
  }

  handleNameClick = () => {
    this.setState({isEditing: true})
  }

  handleInputBlur = reset => e => {
    const {onRename, organization} = this.props

    if (!reset && organization.name !== e.target.value) {
      onRename(organization, e.target.value)
    }

    this.setState({reset: false, isEditing: false})
  }

  handleKeyDown = e => {
    if (e.key === 'Enter') {
      this.inputRef.blur()
    }
    if (e.key === 'Escape') {
      this.setState(
        {reset: true, workingName: this.props.organization.name},
        () => this.inputRef.blur()
      )
    }
  }

  handleFocus = e => {
    e.target.select()
  }

  handleDeleteClick = () => {
    this.setState({isDeleting: true})
  }

  handleDismissDeleteConfirmation = () => {
    this.setState({isDeleting: false})
  }

  handleDeleteOrg = organization => {
    const {onDelete} = this.props
    this.setState({isDeleting: false})
    onDelete(organization)
  }

  render() {
    const {workingName, reset, isEditing, isDeleting} = this.state
    const {organization} = this.props

    return (
      <div className="orgs-table--org">
        <div className="orgs-table--id">
          {organization.id}
        </div>
        {isEditing
          ? <input
              type="text"
              className="form-control input-sm orgs-table--input"
              defaultValue={workingName}
              onBlur={this.handleInputBlur(reset)}
              onKeyDown={this.handleKeyDown}
              placeholder="Name this Organization..."
              autoFocus={true}
              onFocus={this.handleFocus}
              ref={r => (this.inputRef = r)}
            />
          : <div className="orgs-table--name" onClick={this.handleNameClick}>
              {workingName}
              <span className="icon pencil" />
            </div>}
        {isDeleting
          ? <ConfirmButtons
              item={organization}
              onCancel={this.handleDismissDeleteConfirmation}
              onConfirm={this.handleDeleteOrg}
            />
          : <button
              className="btn btn-sm btn-default btn-square"
              onClick={this.handleDeleteClick}
            >
              <span className="icon trash" />
            </button>}
      </div>
    )
  }
}

const {func, shape, string} = PropTypes

Organization.propTypes = {
  organization: shape({
    id: string, // when optimistically created, organization will not have an id
    name: string.isRequired,
  }).isRequired,
  onDelete: func.isRequired,
  onRename: func.isRequired,
}

export default Organization
