import React, {Component} from 'react'
import PropTypes from 'prop-types'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import {getActiveKapacitor} from 'shared/apis'
import * as kapacitorActionCreators from '../actions/view'

import KapacitorRules from 'src/kapacitor/components/KapacitorRules'
import SourceIndicator from 'shared/components/SourceIndicator'
import FancyScrollbar from 'shared/components/FancyScrollbar'
import QuestionMarkTooltip from 'shared/components/QuestionMarkTooltip'

class KapacitorRulesPage extends Component {
  constructor(props) {
    super(props)
    this.state = {
      hasKapacitor: false,
      loading: true,
    }
  }

  async componentDidMount() {
    const kapacitor = await getActiveKapacitor(this.props.source)
    if (!kapacitor) {
      return
    }

    await this.props.actions.fetchRules(kapacitor)
    this.setState({loading: false, hasKapacitor: !!kapacitor})
  }

  handleDeleteRule = rule => {
    const {actions} = this.props

    actions.deleteRule(rule)
  }

  handleRuleStatus = rule => {
    const {actions} = this.props
    const status = rule.status === 'enabled' ? 'disabled' : 'enabled'

    actions.updateRuleStatus(rule, status)
    actions.updateRuleStatusSuccess(rule.id, status)
  }

  render() {
    const {source, rules} = this.props
    const {hasKapacitor, loading} = this.state

    return (
      <PageContents source={source}>
        <KapacitorRules
          source={source}
          rules={rules}
          hasKapacitor={hasKapacitor}
          loading={loading}
          onDelete={this.handleDeleteRule}
          onChangeRuleStatus={this.handleRuleStatus}
        />
      </PageContents>
    )
  }
}

const PageContents = ({children}) => (
  <div className="page">
    <div className="page-header">
      <div className="page-header__container">
        <div className="page-header__left">
          <h1 className="page-header__title">Manage Tasks</h1>
        </div>
        <div className="page-header__right">
          <QuestionMarkTooltip
            tipID="manage-tasks--tooltip"
            tipContent="<b>Alert Rules</b> generate a TICKscript for<br/>you using our Builder UI.<br/><br/>Not all TICKscripts can be edited<br/>using the Builder."
          />
          <SourceIndicator />
        </div>
      </div>
    </div>
    <FancyScrollbar className="page-contents fancy-scroll--kapacitor">
      <div className="container-fluid">
        <div className="row">
          <div className="col-md-12">{children}</div>
        </div>
      </div>
    </FancyScrollbar>
  </div>
)

const {arrayOf, func, node, shape, string} = PropTypes

KapacitorRulesPage.propTypes = {
  source: shape({
    id: string.isRequired,
    links: shape({
      proxy: string.isRequired,
      self: string.isRequired,
      kapacitors: string.isRequired,
    }),
  }),
  rules: arrayOf(
    shape({
      name: string.isRequired,
      trigger: string.isRequired,
      message: string.isRequired,
    })
  ).isRequired,
  actions: shape({
    fetchRules: func.isRequired,
    deleteRule: func.isRequired,
    updateRuleStatus: func.isRequired,
  }).isRequired,
}

PageContents.propTypes = {
  children: node,
}

const mapStateToProps = state => {
  return {
    rules: Object.values(state.rules),
  }
}

const mapDispatchToProps = dispatch => {
  return {
    actions: bindActionCreators(kapacitorActionCreators, dispatch),
  }
}

export default connect(mapStateToProps, mapDispatchToProps)(KapacitorRulesPage)
