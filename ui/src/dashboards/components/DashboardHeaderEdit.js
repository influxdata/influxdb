import React, {PropTypes, Component} from 'react'
import ConfirmButtons from 'shared/components/ConfirmButtons'

class DashboardEditHeader extends Component {
  constructor(props) {
    super(props)

    const {dashboard: {name}} = props
    this.state = {name}
    this.handleChange = ::this.handleChange
  }

  handleChange(name) {
    this.setState({name})
  }

  render() {
    const {onSave, onCancel} = this.props
    const {name} = this.state

    return (
      <div className="page-header full-width">
        <div className="page-header__container">
          <input
            className="page-header--editing"
            autoFocus={true}
            value={name}
            onChange={e => this.handleChange(e.target.value)}
            placeholder="Name this Dashboard"
            spellCheck={false}
            autoComplete={false}
          />
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
