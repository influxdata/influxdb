import React, {PropTypes, Component} from 'react'
import ConfirmButtons from 'src/admin/components/ConfirmButtons'

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
          <div className="page-header__left">
            <input
              className="chronograf-header__editing"
              autoFocus={true}
              value={name}
              placeholder="Dashboard name"
              onChange={(e) => this.handleChange(e.target.value)}
            />
          </div>
          <ConfirmButtons item={name} onConfirm={onSave} onCancel={onCancel} />
        </div>
      </div>
    )
  }
}

const {
  shape,
  func,
} = PropTypes

DashboardEditHeader.propTypes = {
  dashboard: shape({}),
  onCancel: func.isRequired,
  onSave: func.isRequired,
}

export default DashboardEditHeader
