import React, {PropTypes} from 'react'
import DataSection from '../components/DataSection'
import ValuesSection from '../components/ValuesSection'
import RuleHeader from 'src/kapacitor/components/RuleHeader'
import RuleGraph from 'src/kapacitor/components/RuleGraph'
import RuleMessage from 'src/kapacitor/components/RuleMessage'
import {createRule, editRule} from 'src/kapacitor/apis'
import buildInfluxQLQuery from 'utils/influxql'
import timeRanges from 'hson!../../shared/data/timeRanges.hson'

export const KapacitorRule = React.createClass({
  propTypes: {
    source: PropTypes.shape({}).isRequired,
    rule: PropTypes.shape({}).isRequired,
    query: PropTypes.shape({}).isRequired,
    queryConfigs: PropTypes.shape({}).isRequired,
    queryActions: PropTypes.shape({}).isRequired,
    kapacitorActions: PropTypes.shape({}).isRequired,
    addFlashMessage: PropTypes.func.isRequired,
    isEditing: PropTypes.bool.isRequired,
    enabledAlerts: PropTypes.arrayOf(PropTypes.string.isRequired).isRequired,
    router: PropTypes.shape({
      push: PropTypes.func.isRequired,
    }).isRequired,
    kapacitor: PropTypes.shape({}).isRequired,
  },

  getInitialState() {
    const fifteenMinutesIndex = 1
    return {
      timeRange: timeRanges[fifteenMinutesIndex],
    }
  },

  render() {
    const {queryActions, source, enabledAlerts, queryConfigs, query,
      rule, kapacitorActions, isEditing} = this.props
    const {chooseTrigger, updateRuleValues} = kapacitorActions
    const {timeRange} = this.state

    return (
      <div className="page">
        <RuleHeader
          rule={rule}
          actions={kapacitorActions}
          onSave={isEditing ? this.handleEdit : this.handleCreate}
          onChooseTimeRange={this.handleChooseTimeRange}
          validationError={this.validationError()}
          timeRange={timeRange}
          source={source}
        />
        <div className="page-contents page-contents--green-scrollbar">
          <div className="container-fluid">
            <div className="row">
              <div className="col-xs-12">
                <div className="rule-builder">
                  <DataSection timeRange={timeRange} source={source} query={query} actions={queryActions} />
                  <ValuesSection
                    rule={rule}
                    query={queryConfigs[rule.queryID]}
                    onChooseTrigger={chooseTrigger}
                    onUpdateValues={updateRuleValues}
                  />
                  <RuleGraph timeRange={timeRange} source={source} query={query} rule={rule} />
                  <RuleMessage rule={rule} actions={kapacitorActions} enabledAlerts={enabledAlerts} />
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    )
  },

  handleChooseTimeRange({lower}) {
    const timeRange = timeRanges.find((range) => range.lower === lower)
    this.setState({timeRange})
  },

  handleCreate() {
    const {addFlashMessage, queryConfigs, rule, source, router, kapacitor} = this.props

    const newRule = Object.assign({}, rule, {
      query: queryConfigs[rule.queryID],
    })
    delete newRule.queryID

    createRule(kapacitor, newRule).then(() => {
      router.push(`/sources/${source.id}/alert-rules`)
      addFlashMessage({type: 'success', text: `Rule successfully created`})
    }).catch(() => {
      addFlashMessage({type: 'error', text: `There was a problem creating the rule`})
    })
  },

  handleEdit() {
    const {addFlashMessage, queryConfigs, rule} = this.props

    const updatedRule = Object.assign({}, rule, {
      query: queryConfigs[rule.queryID],
    })

    editRule(updatedRule).then(() => {
      addFlashMessage({type: 'success', text: `Rule successfully updated!`})
    }).catch(() => {
      addFlashMessage({type: 'error', text: `There was a problem updating the rule`})
    })
  },

  validationError() {
    if (!buildInfluxQLQuery({}, this.props.query)) {
      return 'Please select a database, measurement, and field'
    }

    if (this.thresholdValueEmpty() || this.relativeValueEmpty()) {
      return 'Please enter a value in the Values section'
    }

    return ''
  },

  thresholdValueEmpty() {
    const {rule} = this.props
    return rule.trigger === 'threshold' && rule.values.value === ''
  },

  relativeValueEmpty() {
    const {rule} = this.props
    return rule.trigger === 'relative' && rule.values.value === ''
  },
})

export default KapacitorRule
