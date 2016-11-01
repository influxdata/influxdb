import React, {PropTypes} from 'react';
import {connect} from 'react-redux';
import DataSection from '../components/DataSection';
import ValuesSection from '../components/ValuesSection';
import * as kapacitorActionCreators from '../actions/view';
import * as queryActionCreators from '../../chronograf/actions/view';
import {bindActionCreators} from 'redux';

const RULE_ID = 1; // switch to this.props.params.taskID

export const KapacitorRulePage = React.createClass({
  propTypes: {
    source: PropTypes.shape({
      links: PropTypes.shape({
        self: PropTypes.string.isRequired,
      }).isRequired,
    }),
    addFlashMessage: PropTypes.func,
    rules: PropTypes.shape({}).isRequired,
    queryConfigs: PropTypes.shape({}).isRequired,
    kapacitorActions: PropTypes.shape({
      fetchRule: PropTypes.func.isRequired,
    }).isRequired,
    queryActions: PropTypes.shape({}).isRequired,
  },

  componentDidMount() {
    this.props.kapacitorActions.fetchRule(RULE_ID);
  },

  render() {
    const rule = this.props.rules[Object.keys(this.props.rules)[0]]; // this.props.params.taskID
    const query = rule && this.props.queryConfigs[rule.queryID];

    if (!query) { // or somethin like that
      return null; // or a spinner or somethin
    }

    return (
      <div className="kapacitor-rule-page">
        <div className="enterprise-header">
          <div className="enterprise-header__container">
            <div className="enterprise-header__left">
              <h1>Kapacitor Rules</h1>
            </div>
          </div>
        </div>
        <div className="container-fluid">
          <div className="row">
            <div className="col-md-12">
              {this.renderDataSection(query)}
            </div>
          </div>
          <div className="row">
            <div className="col-md-12">
              {this.renderValuesSection(rule)}
            </div>
          </div>
          <div className="row">
            <div className="col-md-12">
              {this.renderMessageSection()}
            </div>
          </div>
          <div className="row">
            <div className="col-md-12">
              {this.renderAlertsSection()}
            </div>
          </div>
        </div>
      </div>
    );
  },

  renderDataSection(query) {
    return (
      <div className="kapacitor-rule-section">
        <h3>Data</h3>
        <DataSection source={this.props.source} query={query} actions={this.props.queryActions} />
      </div>
    );
  },

  renderValuesSection(rule) {
    return (
      <div className="kapacitor-rule-section">
        <h3>Values</h3>
        <ValuesSection rule={rule} />
      </div>
    );
  },

  renderMessageSection() {
    return (
      <div className="kapacitor-rule-section">
        <h3>Message</h3>
        <textarea />
      </div>
    );
  },

  renderAlertsSection() {
    // hit kapacitor config endpoint and filter sections by the "enabled" property
    const alertOptions = ['Slack', 'VictorOps'].map((destination) => {
      return <option key={destination}>send to {destination}</option>;
    });
    return (
      <div className="kapacitor-rule-section">
        <h3>Alerts</h3>
        <p>The Alert should <select>{alertOptions}</select></p>
      </div>
    );
  },
});

function mapStateToProps(state) {
  return {
    rules: state.rules,
    queryConfigs: state.queryConfigs,
  };
}

function mapDispatchToProps(dispatch) {
  return {
    kapacitorActions: bindActionCreators(kapacitorActionCreators, dispatch),
    queryActions: bindActionCreators(queryActionCreators, dispatch),
  };
}

export default connect(mapStateToProps, mapDispatchToProps)(KapacitorRulePage);
