import React, {Component, PropTypes} from 'react'

class CreateOrgOverlay extends Component {
  constructor(props) {
    super(props)

    this.state = {
      organizationName: '',
    }
  }

  handleFormSubmit = () => {
    const {onCreateOrg, onDismiss} = this.props
    const {organizationName} = this.state

    onCreateOrg(organizationName)
    onDismiss()
  }

  handleInputChange = e => {
    this.setState({organizationName: e.target.value})
  }

  render() {
    const {onDismiss} = this.props
    const {organizationName} = this.state

    const isOrgNameBlank = organizationName === ''

    return (
      <div className="overlay-technology">
        <div className="create-org-form">
          <div className="create-org-form--header">
            <div className="page-header__left">
              <h1 className="page-header__title">Create Organization</h1>
            </div>
            <div className="page-header__right">
              <span className="page-header__dismiss" onClick={onDismiss} />
            </div>
          </div>
          <div className="create-org-form--body">
            <input
              className="form-control input-md create-org-form--input"
              type="text"
              autoFocus={true}
              placeholder="Name this organization..."
              defaultValue={organizationName}
              onChange={this.handleInputChange}
            />
            <div className="create-org-form--footer">
              <button className="btn btn-sm btn-default" onClick={onDismiss}>
                Cancel
              </button>
              <button
                className="btn btn-sm btn-success"
                onClick={this.handleFormSubmit}
                disabled={isOrgNameBlank}
              >
                Create
              </button>
            </div>
          </div>
        </div>
      </div>
    )
  }
}

const {func} = PropTypes

CreateOrgOverlay.propTypes = {
  onDismiss: func.isRequired,
  onCreateOrg: func.isRequired,
}
export default CreateOrgOverlay
