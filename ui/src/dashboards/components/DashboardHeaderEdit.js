import React, {PropTypes, Component} from 'react'
import {
  DASHBOARD_NAME_MAX_LENGTH,
  NEW_DASHBOARD,
} from 'src/dashboards/constants/index'

class DashboardEditHeader extends Component {
  constructor(props) {
    super(props)

    this.state = {
      reset: false,
    }
  }

  handleInputBlur = e => {
    const {onSave, onCancel} = this.props
    const {reset} = this.state

    if (reset) {
      onCancel()
    } else {
      const newName = e.target.value || NEW_DASHBOARD.name
      onSave(newName)
    }
    this.setState({reset: false})
  }

  handleKeyDown = e => {
    if (e.key === 'Enter') {
      this.inputRef.blur()
    }
    if (e.key === 'Escape') {
      this.inputRef.value = this.props.activeDashboard
      this.setState({reset: true}, () => this.inputRef.blur())
    }
  }

  handleFocus = e => {
    e.target.select()
  }

  render() {
    const {onEditDashboard, isEditMode, activeDashboard} = this.props

    return (
      <div className="dashboard-title">
        {isEditMode
          ? <input
              maxLength={DASHBOARD_NAME_MAX_LENGTH}
              type="text"
              className="dashboard-title--input form-control input-sm"
              defaultValue={activeDashboard}
              autoComplete="off"
              autoFocus={true}
              spellCheck={false}
              onBlur={this.handleInputBlur}
              onKeyDown={this.handleKeyDown}
              onFocus={this.handleFocus}
              placeholder="Name this Dashboard"
              ref={r => (this.inputRef = r)}
            />
          : <h1 onClick={onEditDashboard}>
              {activeDashboard}
            </h1>}
      </div>
    )
  }
}

const {bool, func, string} = PropTypes

DashboardEditHeader.propTypes = {
  activeDashboard: string.isRequired,
  onSave: func.isRequired,
  onCancel: func.isRequired,
  isEditMode: bool,
  onEditDashboard: func.isRequired,
}

export default DashboardEditHeader
