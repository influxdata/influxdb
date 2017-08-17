import React, {PropTypes, Component} from 'react'
import RuleHeaderEdit from 'src/kapacitor/components/RuleHeaderEdit'
import RuleHeaderSave from 'src/kapacitor/components/RuleHeaderSave'

class RuleHeader extends Component {
  constructor(props) {
    super(props)

    this.state = {
      isEditingName: false,
    }
  }

  toggleEditName = () => {
    this.setState({isEditingName: !this.state.isEditingName})
  }

  handleEditName = rule => e => {
    if (e.key === 'Enter') {
      const {updateRuleName} = this.props.actions
      updateRuleName(rule.id, e.target.value)
      this.toggleEditName()
    }

    if (e.key === 'Escape') {
      this.toggleEditName()
    }
  }

  handleEditNameBlur = rule => e => {
    const {updateRuleName} = this.props.actions
    updateRuleName(rule.id, e.target.value)
    this.toggleEditName()
  }

  render() {
    const {
      rule,
      source,
      onSave,
      timeRange,
      validationError,
      onChooseTimeRange,
    } = this.props

    const {isEditingName} = this.state

    return (
      <div className="page-header">
        <div className="page-header__container">
          <RuleHeaderEdit
            rule={rule}
            isEditing={isEditingName}
            onToggleEdit={this.toggleEditName}
            onEditName={this.handleEditName}
            onEditNameBlur={this.handleEditNameBlur}
          />
          <RuleHeaderSave
            source={source}
            onSave={onSave}
            timeRange={timeRange}
            validationError={validationError}
            onChooseTimeRange={onChooseTimeRange}
          />
        </div>
      </div>
    )
  }
}

const {func, shape, string} = PropTypes

RuleHeader.propTypes = {
  source: shape({}).isRequired,
  onSave: func.isRequired,
  rule: shape({}).isRequired,
  actions: shape({
    updateRuleName: func.isRequired,
  }).isRequired,
  validationError: string.isRequired,
  onChooseTimeRange: func.isRequired,
  timeRange: shape({}).isRequired,
}

export default RuleHeader
