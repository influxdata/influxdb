import React, {Component, ChangeEvent} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'
import {InjectedRouter} from 'react-router'

import NameSection from 'src/kapacitor/components/NameSection'
import ValuesSection from 'src/kapacitor/components/ValuesSection'
import RuleHeader from 'src/kapacitor/components/RuleHeader'
import RuleHandlers from 'src/kapacitor/components/RuleHandlers'
import RuleMessage from 'src/kapacitor/components/RuleMessage'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'

import {createRule, editRule} from 'src/kapacitor/apis'
import buildInfluxQLQuery from 'src/utils/influxql'
import {timeRanges} from 'src/shared/data/timeRanges'
import {DEFAULT_RULE_ID} from 'src/kapacitor/constants'
import {notify as notifyAction} from 'src/shared/actions/notifications'

import {
  notifyAlertRuleCreated,
  notifyAlertRuleCreateFailed,
  notifyAlertRuleUpdated,
  notifyAlertRuleUpdateFailed,
  notifyAlertRuleRequiresQuery,
  notifyAlertRuleRequiresConditionValue,
  notifyAlertRuleDeadmanInvalid,
} from 'src/shared/copy/notifications'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {
  Source,
  AlertRule,
  Notification,
  Kapacitor,
  QueryConfig,
  TimeRange,
} from 'src/types'
import {Handler} from 'src/types/kapacitor'
import {
  KapacitorQueryConfigActions,
  KapacitorRuleActions,
} from 'src/types/actions'

interface Props {
  source: Source
  rule: AlertRule
  query: QueryConfig
  queryConfigs: QueryConfig[]
  queryConfigActions: KapacitorQueryConfigActions
  ruleActions: KapacitorRuleActions
  notify: (message: Notification) => void
  ruleID: string
  handlersFromConfig: Handler[]
  router: InjectedRouter
  kapacitor: Kapacitor
  configLink: string
}

interface Item {
  text: string
}

interface TypeItem extends Item {
  type: string
}

interface State {
  timeRange: TimeRange
}

@ErrorHandling
class KapacitorRule extends Component<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      timeRange: timeRanges.find(tr => tr.lower === 'now() - 15m'),
    }
  }

  public render() {
    const {
      rule,
      source,
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
          onSave={this.handleSave}
          validationError={this.validationError}
        />
        <FancyScrollbar className="page-contents fancy-scroll--kapacitor">
          <div className="container-fluid">
            <div className="row">
              <div className="col-xs-12">
                <div className="rule-builder">
                  <NameSection
                    rule={rule}
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
                    validationError={this.validationError}
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

  private handleChooseTimeRange = ({lower}: TimeRange) => {
    const timeRange = timeRanges.find(range => range.lower === lower)
    this.setState({timeRange})
  }

  private handleCreate = (pathname?: string) => {
    const {notify, queryConfigs, rule, source, router, kapacitor} = this.props

    const newRule = Object.assign({}, rule, {
      query: queryConfigs[rule.queryID],
    })
    delete newRule.queryID

    createRule(kapacitor, newRule)
      .then(() => {
        router.push(pathname || `/sources/${source.id}/alert-rules`)
        notify(notifyAlertRuleCreated(newRule.name))
      })
      .catch(e => {
        notify(notifyAlertRuleCreateFailed(newRule.name, e.data.message))
      })
  }

  private handleEdit = (pathname?: string) => {
    const {notify, queryConfigs, rule, router, source} = this.props
    const updatedRule = Object.assign({}, rule, {
      query: queryConfigs[rule.queryID],
    })

    editRule(updatedRule)
      .then(() => {
        router.push(pathname || `/sources/${source.id}/alert-rules`)
        notify(notifyAlertRuleUpdated(rule.name))
      })
      .catch(e => {
        notify(notifyAlertRuleUpdateFailed(rule.name, e.data.message))
      })
  }

  private handleSave = () => {
    const {rule} = this.props
    if (rule.id === DEFAULT_RULE_ID) {
      this.handleCreate()
    } else {
      this.handleEdit()
    }
  }

  private handleSaveToConfig = (configName: string) => () => {
    const {rule, configLink, router} = this.props
    const pathname = `${configLink}#${configName}`

    if (this.validationError) {
      router.push({
        pathname,
      })
      return
    }

    if (rule.id === DEFAULT_RULE_ID) {
      this.handleCreate(pathname)
    } else {
      this.handleEdit(pathname)
    }
  }

  private handleAddEvery = (frequency: string) => {
    const {
      rule: {id: ruleID},
      ruleActions: {addEvery},
    } = this.props
    addEvery(ruleID, frequency)
  }

  private handleRemoveEvery = () => {
    const {
      rule: {id: ruleID},
      ruleActions: {removeEvery},
    } = this.props
    removeEvery(ruleID)
  }

  private get validationError(): string {
    const {rule, query} = this.props
    if (rule.trigger === 'deadman') {
      return this.deadmanValidation()
    }

    if (!buildInfluxQLQuery({lower: ''}, query)) {
      return notifyAlertRuleRequiresQuery()
    }

    if (!rule.values.value) {
      return notifyAlertRuleRequiresConditionValue()
    }

    return ''
  }

  private deadmanValidation = () => {
    const {query} = this.props
    if (query && (!query.database || !query.measurement)) {
      return notifyAlertRuleDeadmanInvalid()
    }

    return ''
  }

  private handleRuleTypeDropdownChange = ({type, text}: TypeItem) => {
    const {ruleActions, rule} = this.props
    ruleActions.updateRuleValues(rule.id, rule.trigger, {
      ...this.props.rule.values,
      [type]: text,
    })
  }

  private handleRuleTypeInputChange = (e: ChangeEvent<HTMLInputElement>) => {
    const {ruleActions, rule} = this.props
    const {lower, upper} = e.target.form

    ruleActions.updateRuleValues(rule.id, rule.trigger, {
      ...this.props.rule.values,
      value: lower.value,
      rangeValue: upper ? upper.value : '',
    })
  }

  private handleDeadmanChange = ({text}: Item) => {
    const {ruleActions, rule} = this.props
    ruleActions.updateRuleValues(rule.id, rule.trigger, {period: text})
  }
}

const mapDispatchToProps = dispatch => ({
  notify: bindActionCreators(notifyAction, dispatch),
})

export default connect(null, mapDispatchToProps)(KapacitorRule)
