import React, {PropTypes} from 'react';
import {connect} from 'react-redux';
import {bindActionCreators} from 'redux';
import {Link} from 'react-router';
import {getKapacitor} from 'src/shared/apis';
import * as kapacitorActionCreators from '../actions/view';
import NoKapacitorError from '../../shared/components/NoKapacitorError';

export const KapacitorRulesPage = React.createClass({
  propTypes: {
    source: PropTypes.shape({
      id: PropTypes.string.isRequired,
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

  getInitialState() {
    return {
      hasKapacitor: false,
      loading: true,
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

  renderSubComponent() {
    const {source} = this.props;
    const {hasKapacitor, loading} = this.state;

    let component;
    if (loading) {
      component = (<p>Loading...</p>);
    } else if (hasKapacitor) {
      component = (
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
      );
    } else {
      component = <NoKapacitorError source={source} />;
    }
    return component;
  },

  render() {
    return (
      <div className="kapacitor-rules-page">
        <div className="enterprise-header">
          <div className="enterprise-header__container">
            <div className="enterprise-header__left">
              <h1>Kapacitor Rules</h1>
            </div>
          </div>
        </div>
        <div className="hosts-page-scroll-container">
          <div className="container-fluid">
            {this.renderSubComponent()}
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
