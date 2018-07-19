import React, {Component} from 'react'
import _ from 'lodash'
import {InjectedRouter} from 'react-router'
import {connect} from 'react-redux'

import * as kapacitorRuleActionCreators from 'src/kapacitor/actions/view'
import * as kapacitorQueryConfigActionCreators from 'src/kapacitor/actions/queryConfigs'

import {bindActionCreators} from 'redux'
import {getActiveKapacitor, getKapacitorConfig} from 'src/shared/apis/index'
import {DEFAULT_RULE_ID} from 'src/kapacitor/constants'
import KapacitorRule from 'src/kapacitor/components/KapacitorRule'
import parseHandlersFromConfig from 'src/shared/parsing/parseHandlersFromConfig'
import {notify as notifyAction} from 'src/shared/actions/notifications'

import {
  notifyKapacitorCreateFailed,
  notifyCouldNotFindKapacitor,
} from 'src/shared/copy/notifications'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {
  Source,
  Notification,
  AlertRule,
  QueryConfig,
  Kapacitor,
} from 'src/types'
import {
  KapacitorQueryConfigActions,
  KapacitorRuleActions,
} from 'src/types/actions'

interface Params {
  ruleID: string
}

interface Props {
  source: Source
  notify: (notification: Notification) => void
  rules: AlertRule[]
  queryConfigs: QueryConfig[]
  ruleActions: KapacitorRuleActions
  queryConfigActions: KapacitorQueryConfigActions
  params: Params
  router: InjectedRouter
}

interface State {
  handlersFromConfig: any[]
  kapacitor: Kapacitor | {}
}

@ErrorHandling
class KapacitorRulePage extends Component<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      handlersFromConfig: [],
      kapacitor: {},
    }
  }

  public async componentDidMount() {
    const {params, source, ruleActions, notify} = this.props

    if (params.ruleID === 'new') {
      ruleActions.loadDefaultRule()
    } else {
      ruleActions.fetchRule(source, params.ruleID)
    }

    const kapacitor = await getActiveKapacitor(this.props.source)
    if (!kapacitor) {
      return notify(notifyCouldNotFindKapacitor())
    }

    try {
      const kapacitorConfig = await getKapacitorConfig(kapacitor)
      const handlersFromConfig = parseHandlersFromConfig(kapacitorConfig)
      this.setState({kapacitor, handlersFromConfig})
    } catch (error) {
      notify(notifyKapacitorCreateFailed())
      console.error(error)
      throw error
    }
  }

  public render() {
    const {
      params,
      source,
      router,
      ruleActions,
      queryConfigs,
      queryConfigActions,
    } = this.props
    const {handlersFromConfig, kapacitor} = this.state
    const rule = this.rule
    const query = rule && queryConfigs[rule.queryID]

    if (!query) {
      return <div className="page-spinner" />
    }
    return (
      <KapacitorRule
        source={source}
        rule={rule}
        query={query}
        queryConfigs={queryConfigs}
        queryConfigActions={queryConfigActions}
        ruleActions={ruleActions}
        handlersFromConfig={handlersFromConfig}
        ruleID={params.ruleID}
        router={router}
        kapacitor={kapacitor}
        configLink={`/sources/${source.id}/kapacitors/${this.kapacitorID}/edit`}
      />
    )
  }

  private get kapacitorID(): string {
    const {kapacitor} = this.state
    return _.get(kapacitor, 'id')
  }

  private get rule(): AlertRule {
    const {params, rules} = this.props
    const ruleID = _.get(params, 'ruleID')

    if (ruleID === 'new') {
      return rules[DEFAULT_RULE_ID]
    }

    return rules[params.ruleID]
  }
}

const mapStateToProps = ({rules, kapacitorQueryConfigs: queryConfigs}) => ({
  rules,
  queryConfigs,
})

const mapDispatchToProps = dispatch => ({
  ruleActions: bindActionCreators(kapacitorRuleActionCreators, dispatch),
  notify: bindActionCreators(notifyAction, dispatch),
  queryConfigActions: bindActionCreators(
    kapacitorQueryConfigActionCreators,
    dispatch
  ),
})

export default connect(mapStateToProps, mapDispatchToProps)(KapacitorRulePage)
