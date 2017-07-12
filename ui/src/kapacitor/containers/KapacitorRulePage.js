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

    getKapacitorConfig(kapacitor)
      .then(({data: {sections}}) => {
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
      })
      .catch(() => {
        addFlashMessage({
          type: 'error',
          text: 'There was a problem communicating with Kapacitor',
        })
      })
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

function mapStateToProps(state) {
  return {
    rules: state.rules,
    queryConfigs: state.queryConfigs,
  }
}

function mapDispatchToProps(dispatch) {
  return {
    kapacitorActions: bindActionCreators(kapacitorActionCreators, dispatch),
    queryActions: bindActionCreators(queryActionCreators, dispatch),
  }
}

KapacitorRulePage.propTypes = {
  source: PropTypes.shape({
    links: PropTypes.shape({
      proxy: PropTypes.string.isRequired,
      self: PropTypes.string.isRequired,
    }),
  }),
  addFlashMessage: PropTypes.func,
  rules: PropTypes.shape({}).isRequired,
  queryConfigs: PropTypes.shape({}).isRequired,
  kapacitorActions: PropTypes.shape({
    loadDefaultRule: PropTypes.func.isRequired,
    fetchRule: PropTypes.func.isRequired,
    chooseTrigger: PropTypes.func.isRequired,
    addEvery: PropTypes.func.isRequired,
    removeEvery: PropTypes.func.isRequired,
    updateRuleValues: PropTypes.func.isRequired,
    updateMessage: PropTypes.func.isRequired,
    updateAlerts: PropTypes.func.isRequired,
    updateRuleName: PropTypes.func.isRequired,
  }).isRequired,
  queryActions: PropTypes.shape({}).isRequired,
  params: PropTypes.shape({
    ruleID: PropTypes.string,
  }).isRequired,
  router: PropTypes.shape({
    push: PropTypes.func.isRequired,
  }).isRequired,
}

export default connect(mapStateToProps, mapDispatchToProps)(KapacitorRulePage)
