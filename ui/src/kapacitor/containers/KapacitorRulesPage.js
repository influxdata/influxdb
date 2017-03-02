import React, {PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'
import {getKapacitor} from 'src/shared/apis'
import * as kapacitorActionCreators from '../actions/view'
import KapacitorRules from 'src/kapacitor/components/KapacitorRules'

const {
  arrayOf,
  func,
  shape,
  string,
} = PropTypes

export const KapacitorRulesPage = React.createClass({
  propTypes: {
    source: shape({
      id: string.isRequired,
      links: shape({
        proxy: string.isRequired,
        self: string.isRequired,
        kapacitors: string.isRequired,
      }),
    }),
    rules: arrayOf(shape({
      name: string.isRequired,
      trigger: string.isRequired,
      message: string.isRequired,
      alerts: arrayOf(string.isRequired).isRequired,
    })).isRequired,
    actions: shape({
      fetchRules: func.isRequired,
      deleteRule: func.isRequired,
      updateRuleStatus: func.isRequired,
    }).isRequired,
    addFlashMessage: func,
  },

  getInitialState() {
    return {
      hasKapacitor: false,
      loading: true,
      fooRule: 'HAI',
    };
  },

  componentDidMount() {
    getKapacitor(this.props.source).then((kapacitor) => {
      if (kapacitor) {
        this.props.actions.fetchRules(kapacitor);
      }
      this.setState({loading: false, hasKapacitor: !!kapacitor});
    });
  },

  handleDeleteRule(rule) {
    const {actions} = this.props;
    actions.deleteRule(rule);
  },

  handleRuleStatus(rule) {
    const {actions} = this.props
    const status = rule.status === 'enabled' ? 'disabled' : 'enabled'

    actions.updateRuleStatus(rule, status)
    actions.updateRuleStatusSuccess(rule.id, status)
  },

  render() {
    const {source, rules} = this.props
    const {hasKapacitor, loading} = this.state

    return (
      <KapacitorRules
        source={source}
        rules={rules}
        hasKapacitor={hasKapacitor}
        loading={loading}
        onDelete={this.handleDeleteRule}
        onChangeRuleStatus={this.handleRuleStatus}
      />
    )
  },
});

function mapStateToProps(state) {
  return {
    rules: Object.values(state.rules),
  };
}

function mapDispatchToProps(dispatch) {
  return {
    actions: bindActionCreators(kapacitorActionCreators, dispatch),
  };
}

export default connect(mapStateToProps, mapDispatchToProps)(KapacitorRulesPage);
