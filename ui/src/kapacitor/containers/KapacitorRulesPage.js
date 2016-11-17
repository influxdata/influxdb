import React, {PropTypes} from 'react';
import {connect} from 'react-redux';
import {bindActionCreators} from 'redux';
import {Link} from 'react-router';
import * as kapacitorActionCreators from 'src/kapacitor/actions/view';

export const KapacitorRulesPage = React.createClass({
  propTypes: {
    source: PropTypes.shape({
      links: PropTypes.shape({
        proxy: PropTypes.string.isRequired,
        self: PropTypes.string.isRequired,
        kapacitors: PropTypes.string.isRequired,
      }),
    }),
    rules: PropTypes.arrayOf(PropTypes.shape({
      name: PropTypes.string.isRequired,
      trigger: PropTypes.string.isRequired,
      message: PropTypes.string.isRequired,
      alerts: PropTypes.arrayOf(PropTypes.string.isRequired).isRequired,
    })).isRequired,
    actions: PropTypes.shape({
      fetchRules: PropTypes.func.isRequired,
      deleteRule: PropTypes.func.isRequired,
    }).isRequired,
    addFlashMessage: PropTypes.func,
  },

  componentDidMount() {
    this.props.actions.fetchRules(this.props.source);
  },

  handleDeleteRule(rule) {
    const {actions} = this.props;
    actions.deleteRule(rule);
  },

  render() {
    const {source} = this.props;

    return (
      <div className="kapacitor-rules-page">
        <div className="chronograf-header">
          <div className="chronograf-header__container">
            <div className="chronograf-header__left">
              <h1>Kapacitor Rules</h1>
            </div>
          </div>
        </div>
        <div className="hosts-page-scroll-container">
          <div className="container-fluid">
            <div className="panel panel-minimal">
              <div className="panel-heading u-flex u-ai-center u-jc-space-between">
                <h2 className="panel-title">Alert Rules</h2>
                <Link to={`/sources/${source.id}/alert-rules/new`} className="btn btn-sm btn-primary">Create New Rule</Link>
              </div>
              <div className="panel-body">
                <table className="table v-center">
                  <thead>
                    <tr>
                      <th>Name</th>
                      <th>Trigger</th>
                      <th>Message</th>
                      <th>Alerts</th>
                      <th></th>
                    </tr>
                  </thead>
                  <tbody>
                    {this.renderAlertsTableRows()}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  },

  renderAlertsTableRows() {
    const {rules, source} = this.props;
    const numRules = rules.length;

    if (numRules === 0) {
      return (
        <tr className="table-empty-state">
          <th colSpan="5">
            <p>You don&#39;t have any Kapacitor<br/>Rules, why not create one?</p>
            <Link to={`/sources/${source.id}/alert-rules/new`} className="btn btn-primary">Create New Rule</Link>
          </th>
        </tr>
      );
    }

    return rules.map((rule) => {
      return (
        <tr key={rule.id}>
          <td className="monotype"><Link to={`/sources/${source.id}/alert-rules/${rule.id}`}>{rule.name}</Link></td>
          <td className="monotype">{rule.trigger}</td>
          <td className="monotype">{rule.message}</td>
          <td className="monotype">{rule.alerts.join(', ')}</td>
          <td><button className="btn btn-danger btn-xs" onClick={() => this.handleDeleteRule(rule)}>Delete</button></td>
        </tr>
      );
    });
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
