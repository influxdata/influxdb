import React, {PropTypes, Component} from 'react'
import ConfirmButtons from 'shared/components/ConfirmButtons'

class DashboardEditHeader extends Component {
  constructor(props) {
    super(props)

    const {dashboard: {name}} = props
    this.state = {name}
    this.handleChange = ::this.handleChange
    this.handleFormSubmit = ::this.handleFormSubmit
    this.handleKeyUp = ::this.handleKeyUp
  }

  handleChange(name) {
    this.setState({name})
  }

  handleFormSubmit(e) {
    e.preventDefault()
    const name = e.target.name.value
    this.props.onSave(name)
  }

  handleKeyUp(e) {
    const {onCancel} = this.props
    if (e.key === 'Escape') {
      onCancel()
    }
  }

  render() {
    const {onSave, onCancel} = this.props
    const {name} = this.state

    return (
      <div className="page-header full-width">
        <div className="page-header__container">
          <form className="page-header__left" onSubmit={this.handleFormSubmit}>
            <input
              className="page-header--editing"
              name="name"
              autoFocus={true}
              value={name}
              placeholder="Name this Dashboard"
              onChange={e => this.handleChange(e.target.value)}
              onKeyUp={this.handleKeyUp}
              autoComplete="off"
            />
          </form>
          <ConfirmButtons item={name} onConfirm={onSave} onCancel={onCancel} />
        </div>
      </div>
    )
  }
}

const {shape, func} = PropTypes

DashboardEditHeader.propTypes = {
  dashboard: shape({}),
  onCancel: func.isRequired,
  onSave: func.isRequired,
}

export default DashboardEditHeader
