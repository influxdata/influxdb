import React, {PropTypes} from 'react'
import ReactTooltip from 'react-tooltip'
import TimeRangeDropdown from 'shared/components/TimeRangeDropdown'
import SourceIndicator from 'shared/components/SourceIndicator'

export const RuleHeader = React.createClass({
  propTypes: {
    source: PropTypes.shape({}).isRequired,
    onSave: PropTypes.func.isRequired,
    rule: PropTypes.shape({}).isRequired,
    actions: PropTypes.shape({
      updateRuleName: PropTypes.func.isRequired,
    }).isRequired,
    validationError: PropTypes.string.isRequired,
    onChooseTimeRange: PropTypes.func.isRequired,
    timeRange: PropTypes.shape({}).isRequired,
  },

  getInitialState() {
    return {
      isEditingName: false,
    }
  },

  handleEditName(e, rule) {
    if (e.key === 'Enter') {
      const {updateRuleName} = this.props.actions
      const name = this.ruleName.value
      updateRuleName(rule.id, name)
      this.toggleEditName()
    }

    if (e.key === 'Escape') {
      this.toggleEditName()
    }
  },

  handleEditNameBlur(rule) {
    const {updateRuleName} = this.props.actions
    const name = this.ruleName.value
    updateRuleName(rule.id, name)
    this.toggleEditName()
  },

  toggleEditName() {
    this.setState({isEditingName: !this.state.isEditingName})
  },

  render() {
    return (
      <div className="page-header">
        <div className="page-header__container">
          {this.renderEditName()}
          {this.renderSave()}
        </div>
      </div>
    )
  },

  renderSave() {
    const {
      validationError,
      onSave,
      timeRange,
      onChooseTimeRange,
      source,
    } = this.props
    const saveButton = validationError
      ? <button
          className="btn btn-success btn-sm disabled"
          data-for="save-kapacitor-tooltip"
          data-tip={validationError}
        >
          Save Rule
        </button>
      : <button className="btn btn-success btn-sm" onClick={onSave}>
          Save Rule
        </button>

    return (
      <div className="page-header__right">
        <SourceIndicator sourceName={source.name} />
        <TimeRangeDropdown
          onChooseTimeRange={onChooseTimeRange}
          selected={timeRange}
          preventCustomTimeRange={true}
        />
        {saveButton}
        <ReactTooltip
          id="save-kapacitor-tooltip"
          effect="solid"
          html={true}
          offset={{bottom: 4}}
          place="bottom"
          class="influx-tooltip kapacitor-tooltip place-bottom"
        />
      </div>
    )
  },

  renderEditName() {
    const {rule} = this.props
    const {isEditingName} = this.state

    const name = isEditingName
      ? <input
          className="page-header--editing kapacitor-theme"
          autoFocus={true}
          defaultValue={rule.name}
          ref={r => (this.ruleName = r)}
          onKeyDown={e => this.handleEditName(e, rule)}
          onBlur={() => this.handleEditNameBlur(rule)}
          placeholder="Name your rule"
          spellCheck={false}
          autoComplete={false}
        />
      : <div className="page-header__left">
          <h1
            className="page-header__title page-header--editable kapacitor-theme"
            onClick={this.toggleEditName}
            data-for="rename-kapacitor-tooltip"
            data-tip="Click to Rename"
          >
            {rule.name}
            <span className="icon pencil" />
            <ReactTooltip
              id="rename-kapacitor-tooltip"
              delayShow={200}
              effect="solid"
              html={true}
              offset={{top: 2}}
              place="bottom"
              class="influx-tooltip kapacitor-tooltip place-bottom"
            />
          </h1>
        </div>

    return name
  },
})

export default RuleHeader
