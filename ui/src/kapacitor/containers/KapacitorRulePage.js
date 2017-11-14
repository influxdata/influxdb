import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

import * as kapacitorRuleActionCreators from 'src/kapacitor/actions/view'
import * as kapacitorQueryConfigActionCreators from 'src/kapacitor/actions/queryConfigs'

import {bindActionCreators} from 'redux'
import {getActiveKapacitor, getKapacitorConfig} from 'shared/apis/index'
import {
  DEFAULT_RULE_ID,
  ALERT_FIELDS_FROM_CONFIG,
} from 'src/kapacitor/constants'
import KapacitorRule from 'src/kapacitor/components/KapacitorRule'

const getEnabled = config => {
  const {data: {sections}} = config
  const allAlerts = _.map(sections, (v, k) => {
    const fromConfig = _.get(v, ['elements', '0', 'options'], {})
    const pickedFromConfig = _.pick(fromConfig, [
      ALERT_FIELDS_FROM_CONFIG[k],
      'enabled',
    ])
    return {type: k, ...pickedFromConfig}
  })
  let enabledAlerts = _.filter(allAlerts, v => _.get(v, ['enabled'], false))
  enabledAlerts = _.reject(enabledAlerts, v => v.type === 'influxdb') // TODO: remove this.
  return enabledAlerts
}

class KapacitorRulePage extends Component {
  constructor(props) {
    super(props)

    this.state = {
      enabledAlerts: [],
      kapacitor: {},
    }
  }

  async componentDidMount() {
    const {params, source, ruleActions, addFlashMessage} = this.props
    params.ruleID === 'new'
      ? ruleActions.loadDefaultRule()
      : ruleActions.fetchRule(source, params.ruleID)

    const kapacitor = await getActiveKapacitor(this.props.source)
    if (!kapacitor) {
      return addFlashMessage({
        type: 'error',
        text: "We couldn't find a configured Kapacitor for this source", // eslint-disable-line quotes
      })
    }

    try {
      const kapacitorConfig = await getKapacitorConfig(kapacitor)
      const enabledAlerts = getEnabled(kapacitorConfig)
      this.setState({kapacitor, enabledAlerts})
    } catch (error) {
      addFlashMessage({
        type: 'error',
        text: 'There was a problem communicating with Kapacitor',
      })
      console.error(error)
      throw error
    }
  }

  render() {
    const {
      rules,
      queryConfigs,
      params,
      ruleActions,
      source,
      queryConfigActions,
      addFlashMessage,
      router,
    } = this.props
    const {enabledAlerts, kapacitor} = this.state
    const rule =
      params.ruleID === 'new' ? rules[DEFAULT_RULE_ID] : rules[params.ruleID]
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
        addFlashMessage={addFlashMessage}
        enabledAlerts={enabledAlerts}
        ruleID={params.ruleID}
        router={router}
        kapacitor={kapacitor}
      />
    )
  }
}

const {func, shape, string} = PropTypes

KapacitorRulePage.propTypes = {
  source: shape({
    links: shape({
      proxy: string.isRequired,
      self: string.isRequired,
    }),
  }),
  addFlashMessage: func,
  rules: shape({}).isRequired,
  queryConfigs: shape({}).isRequired,
  ruleActions: shape({
    loadDefaultRule: func.isRequired,
    fetchRule: func.isRequired,
    chooseTrigger: func.isRequired,
    addEvery: func.isRequired,
    removeEvery: func.isRequired,
    updateRuleValues: func.isRequired,
    updateMessage: func.isRequired,
    updateAlerts: func.isRequired,
    updateRuleName: func.isRequired,
  }).isRequired,
  queryConfigActions: shape({}).isRequired,
  params: shape({
    ruleID: string,
  }).isRequired,
  router: shape({
    push: func.isRequired,
  }).isRequired,
}

const mapStateToProps = ({rules, kapacitorQueryConfigs: queryConfigs}) => ({
  rules,
  queryConfigs,
})

const mapDispatchToProps = dispatch => ({
  ruleActions: bindActionCreators(kapacitorRuleActionCreators, dispatch),
  queryConfigActions: bindActionCreators(
    kapacitorQueryConfigActionCreators,
    dispatch
  ),
})

export default connect(mapStateToProps, mapDispatchToProps)(KapacitorRulePage)
