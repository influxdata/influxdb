import React, {PropTypes, Component} from 'react'

import NameSection from 'src/kapacitor/components/NameSection'
import ValuesSection from 'src/kapacitor/components/ValuesSection'
import RuleHeader from 'src/kapacitor/components/RuleHeader'
import RuleHandlers from 'src/kapacitor/components/RuleHandlers'
import RuleMessage from 'src/kapacitor/components/RuleMessage'
import FancyScrollbar from 'shared/components/FancyScrollbar'

import {createRule, editRule} from 'src/kapacitor/apis'
import buildInfluxQLQuery from 'utils/influxql'
import timeRanges from 'hson!shared/data/timeRanges.hson'

class KapacitorRule extends Component {
  constructor(props) {
    super(props)
    this.state = {
      timeRange: timeRanges.find(tr => tr.lower === 'now() - 15m'),
    }
  }

  handleChooseTimeRange = ({lower}) => {
    const timeRange = timeRanges.find(range => range.lower === lower)
    this.setState({timeRange})
  }

  handleCreate = () => {
    const {
      addFlashMessage,
      queryConfigs,
      rule,
      source,
      router,
      kapacitor,
    } = this.props

    const newRule = Object.assign({}, rule, {
      query: queryConfigs[rule.queryID],
    })
    delete newRule.queryID

    createRule(kapacitor, newRule)
      .then(() => {
        router.push(`/sources/${source.id}/alert-rules`)
        addFlashMessage({type: 'success', text: 'Rule successfully created'})
      })
      .catch(() => {
        addFlashMessage({
          type: 'error',
          text: 'There was a problem creating the rule',
        })
      })
  }

  handleEdit = () => {
    const {addFlashMessage, queryConfigs, rule, router, source} = this.props
    const updatedRule = Object.assign({}, rule, {
      query: queryConfigs[rule.queryID],
    })

    editRule(updatedRule)
      .then(() => {
        router.push(`/sources/${source.id}/alert-rules`)
        addFlashMessage({
          type: 'success',
          text: `${rule.name} successfully saved!`,
        })
      })
      .catch(e => {
        addFlashMessage({
          type: 'error',
          text: `There was a problem saving ${rule.name}: ${e.data.message}`,
        })
      })
  }

  handleSaveToConfig = () => {
    const {
      addFlashMessage,
      queryConfigs,
      rule,
      router,
      configLink,
      kapacitor,
    } = this.props
    const updatedRule = Object.assign({}, rule, {
      query: queryConfigs[rule.queryID],
    })
    if (this.validationError()) {
      router.push(configLink)
    } else if (rule.id === 'new') {
      const newRule = Object.assign({}, rule, {
        query: queryConfigs[rule.queryID],
      })
      delete newRule.queryID

      createRule(kapacitor, newRule)
        .then(() => {
          router.push(configLink)
          addFlashMessage({
            type: 'success',
            text: 'Rule successfully created',
          })
        })
        .catch(() => {
          addFlashMessage({
            type: 'error',
            text: 'There was a problem creating the rule',
          })
        })
    } else {
      editRule(updatedRule)
        .then(() => {
          router.push(configLink)
          addFlashMessage({
            type: 'success',
            text: `${rule.name} successfully saved!`,
          })
        })
        .catch(e => {
          addFlashMessage({
            type: 'error',
            text: `There was a problem saving ${rule.name}: ${e.data.message}`,
          })
        })
    }
  }

  handleAddEvery = frequency => {
    const {rule: {id: ruleID}, ruleActions: {addEvery}} = this.props
    addEvery(ruleID, frequency)
  }

  handleRemoveEvery = () => {
    const {rule: {id: ruleID}, ruleActions: {removeEvery}} = this.props
    removeEvery(ruleID)
  }

  validationError = () => {
    const {rule, query} = this.props
    if (rule.trigger === 'deadman') {
      return this.deadmanValidation()
    }

    if (!buildInfluxQLQuery({}, query)) {
      return 'Please select a Database, Measurement, and Field'
    }

    if (!rule.values.value) {
      return 'Please enter a value in the Conditions section'
    }

    return ''
  }

  deadmanValidation = () => {
    const {query} = this.props
    if (query && (!query.database || !query.measurement)) {
      return 'Deadman rules require a Database and Measurement'
    }

    return ''
  }

  handleRuleTypeDropdownChange = ({type, text}) => {
    const {ruleActions, rule} = this.props
    ruleActions.updateRuleValues(rule.id, rule.trigger, {
      ...this.props.rule.values,
      [type]: text,
    })
  }

  handleRuleTypeInputChange = e => {
    const {ruleActions, rule} = this.props
    const {lower, upper} = e.target.form

    ruleActions.updateRuleValues(rule.id, rule.trigger, {
      ...this.props.rule.values,
      value: lower.value,
      rangeValue: upper ? upper.value : '',
    })
  }

  handleDeadmanChange = ({text}) => {
    const {ruleActions, rule} = this.props
    ruleActions.updateRuleValues(rule.id, rule.trigger, {period: text})
  }

  render() {
    const {
      rule,
      source,
      ruleID,
      ruleActions,
      queryConfigs,
      handlersFromConfig,
      queryConfigActions,
    } = this.props
    const {chooseTrigger, updateRuleValues} = ruleActions
    const {timeRange} = this.state

    return (
      <div className="page">
        <RuleHeader
          source={source}
          onSave={ruleID === 'new' ? this.handleCreate : this.handleEdit}
          validationError={this.validationError()}
        />
        <FancyScrollbar className="page-contents fancy-scroll--kapacitor">
          <div className="container-fluid">
            <div className="row">
              <div className="col-xs-12">
                <div className="rule-builder">
                  <NameSection
                    ruleID={ruleID}
                    defaultName={rule.name}
                    onRuleRename={ruleActions.updateRuleName}
                  />
                  <ValuesSection
                    rule={rule}
                    source={source}
                    timeRange={timeRange}
                    onChooseTrigger={chooseTrigger}
                    onAddEvery={this.handleAddEvery}
                    onUpdateValues={updateRuleValues}
                    query={queryConfigs[rule.queryID]}
                    onRemoveEvery={this.handleRemoveEvery}
                    queryConfigActions={queryConfigActions}
                    onDeadmanChange={this.handleDeadmanChange}
                    onRuleTypeInputChange={this.handleRuleTypeInputChange}
                    onRuleTypeDropdownChange={this.handleRuleTypeDropdownChange}
                    onChooseTimeRange={this.handleChooseTimeRange}
                  />
                  <RuleHandlers
                    rule={rule}
                    ruleActions={ruleActions}
                    handlersFromConfig={handlersFromConfig}
                    onGoToConfig={this.handleSaveToConfig}
                    validationError={this.validationError()}
                  />
                  <RuleMessage rule={rule} ruleActions={ruleActions} />
                </div>
              </div>
            </div>
          </div>
        </FancyScrollbar>
      </div>
    )
  }
}

const {arrayOf, func, shape, string} = PropTypes

KapacitorRule.propTypes = {
  source: shape({}).isRequired,
  rule: shape({
    values: shape({}),
  }).isRequired,
  query: shape({}).isRequired,
  queryConfigs: shape({}).isRequired,
  queryConfigActions: shape({}).isRequired,
  ruleActions: shape({}).isRequired,
  addFlashMessage: func.isRequired,
  ruleID: string.isRequired,
  handlersFromConfig: arrayOf(shape({})).isRequired,
  router: shape({
    push: func.isRequired,
  }).isRequired,
  kapacitor: shape({}).isRequired,
  configLink: string.isRequired,
}

export default KapacitorRule
