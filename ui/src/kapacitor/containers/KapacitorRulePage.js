import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

import * as kapacitorActionCreators from 'src/kapacitor/actions/view'
import * as queryActionCreators from 'src/data_explorer/actions/view'

import {bindActionCreators} from 'redux'
import {getActiveKapacitor, getKapacitorConfig} from 'shared/apis/index'
import {ALERTS, DEFAULT_RULE_ID} from 'src/kapacitor/constants'
import KapacitorRule from 'src/kapacitor/components/KapacitorRule'

class KapacitorRulePage extends Component {
  constructor(props) {
    super(props)

    this.state = {
      enabledAlerts: [],
      kapacitor: {},
    }

    this.isEditing = ::this.isEditing
  }

  async componentDidMount() {
    const {params, source, kapacitorActions, addFlashMessage} = this.props
    if (this.isEditing()) {
      kapacitorActions.fetchRule(source, params.ruleID)
    } else {
      kapacitorActions.loadDefaultRule()
    }

    const kapacitor = await getActiveKapacitor(this.props.source)
    if (!kapacitor) {
      return addFlashMessage({
        type: 'error',
        text: "We couldn't find a configured Kapacitor for this source", // eslint-disable-line quotes
      })
    }

    try {
      const {data: {sections}} = await getKapacitorConfig(kapacitor)
      const enabledAlerts = Object.keys(sections).filter(section => {
        return (
          _.get(
            sections,
            [section, 'elements', '0', 'options', 'enabled'],
            false
          ) && ALERTS.includes(section)
        )
      })

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
      kapacitorActions,
      source,
      queryActions,
      addFlashMessage,
      router,
    } = this.props
    const {enabledAlerts, kapacitor} = this.state

    const rule = this.isEditing()
      ? rules[params.ruleID]
      : rules[DEFAULT_RULE_ID]
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
        queryActions={queryActions}
        kapacitorActions={kapacitorActions}
        addFlashMessage={addFlashMessage}
        enabledAlerts={enabledAlerts}
        isEditing={this.isEditing()}
        router={router}
        kapacitor={kapacitor}
      />
    )
  }

  isEditing() {
    const {params} = this.props
    return params.ruleID && params.ruleID !== 'new'
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
  kapacitorActions: shape({
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
  queryActions: shape({}).isRequired,
  params: shape({
    ruleID: string,
  }).isRequired,
  router: shape({
    push: func.isRequired,
  }).isRequired,
}

const mapStateToProps = state => {
  return {
    rules: state.rules,
    queryConfigs: state.queryConfigs,
  }
}

const mapDispatchToProps = dispatch => {
  return {
    kapacitorActions: bindActionCreators(kapacitorActionCreators, dispatch),
    queryActions: bindActionCreators(queryActionCreators, dispatch),
  }
}

export default connect(mapStateToProps, mapDispatchToProps)(KapacitorRulePage)
