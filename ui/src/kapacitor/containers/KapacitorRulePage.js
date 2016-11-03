import React, {PropTypes} from 'react';
import {connect} from 'react-redux';
import DataSection from '../components/DataSection';
import ValuesSection from '../components/ValuesSection';
import * as kapacitorActionCreators from '../actions/view';
import * as queryActionCreators from '../../chronograf/actions/view';
import {bindActionCreators} from 'redux';
import selectStatement from 'src/chronograf/utils/influxql/select';
import AutoRefresh from 'shared/components/AutoRefresh';
import LineGraph from 'shared/components/LineGraph';
const RefreshingLineGraph = AutoRefresh(LineGraph);

export const KapacitorRulePage = React.createClass({
  propTypes: {
    source: PropTypes.shape({
      links: PropTypes.shape({
        proxy: PropTypes.string.isRequired,
        self: PropTypes.string.isRequired,
      }).isRequired,
    }),
    addFlashMessage: PropTypes.func,
    rules: PropTypes.shape({}).isRequired,
    queryConfigs: PropTypes.shape({}).isRequired,
    kapacitorActions: PropTypes.shape({
      loadDefaultRule: PropTypes.func.isRequired,
      fetchRule: PropTypes.func.isRequired,
      chooseTrigger: PropTypes.func.isRequired,
      updateRuleValues: PropTypes.func.isRequired,
      updateMessage: PropTypes.func.isRequired,
    }).isRequired,
    queryActions: PropTypes.shape({}).isRequired,
    params: PropTypes.shape({
      ruleID: PropTypes.string,
    }).isRequired,
  },

  componentDidMount() {
    const {ruleID} = this.props.params;
    if (ruleID) {
      this.props.kapacitorActions.fetchRule(ruleID);
    } else {
      this.props.kapacitorActions.loadDefaultRule();
    }
  },

  handleSave() {
    console.log(this.props.rules); // eslint-disable-line no-console
  },

  createUnderlayCallback(rule) {
    return (canvas, area, dygraph) => {
      if (rule.trigger !== 'threshold') {
        return;
      }
      const theOnePercent = 0.01;
      let highlightStart = 0;
      let highlightEnd = 0;

      switch (rule.values.operator) {
        case 'greater than': {
          highlightStart = rule.values.value;
          highlightEnd = dygraph.yAxisRange()[1];
          break;
        }

        case 'less than': {
          highlightStart = dygraph.yAxisRange()[0];
          highlightEnd = rule.values.value;
          break;
        }

        case 'equal to': {
          const width = (theOnePercent) * (dygraph.yAxisRange()[1] - dygraph.yAxisRange()[0]);
          highlightStart = +rule.values.value - width;
          highlightEnd = +rule.values.value + width;
          break;
        }
        case 'not equal to': {
          const width = (theOnePercent) * (dygraph.yAxisRange()[1] - dygraph.yAxisRange()[0]);
          highlightStart = +rule.values.value - width;
          highlightEnd = +rule.values.value + width;
          break;
        }
      }

      const bottom = dygraph.toDomYCoord(highlightStart);
      const top = dygraph.toDomYCoord(highlightEnd);

      canvas.fillStyle = 'rgba(220,20,60, 1)';
      canvas.fillRect(area.x, top, area.w, bottom - top);
    };
  },

  render() {
    const {rules, queryConfigs, source} = this.props;
    const rule = rules[Object.keys(rules)[0]]; // this.props.params.taskID
    const query = rule && queryConfigs[rule.queryID];
    const autoRefreshMs = 30000;

    if (!query) { // or somethin like that
      return null; // or a spinner or somethin
    }

    const queryText = selectStatement({lower: 'now() - 15m'}, query);
    const queries = [{host: source.links.proxy, text: queryText}];

    return (
      <div className="kapacitor-rule-page">
        <div className="enterprise-header">
          <div className="enterprise-header__container">
            <div className="enterprise-header__left">
              <h1>Kapacitor Rules</h1>
            </div>
            <div className="enterprise-header__right">
              <button className="btn btn-primary btn-sm" onClick={this.handleSave}>Save</button>
            </div>
          </div>
        </div>
        <div className="container-fluid">
          <div className="row">
            <div className="col-md-12">
              {
                queryText ?
                  <RefreshingLineGraph
                    queries={queries}
                    autoRefresh={autoRefreshMs}
                    underlayCallback={this.createUnderlayCallback(rule)}
                  />
                : null
              }
            </div>
          </div>
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
              {this.renderMessageSection(rule)}
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
    const {chooseTrigger, updateRuleValues} = this.props.kapacitorActions;
    return (
      <div className="kapacitor-rule-section">
        <h3>Values</h3>
        <ValuesSection rule={rule} onChooseTrigger={chooseTrigger} onUpdateValues={updateRuleValues} />
      </div>
    );
  },

  renderMessageSection(rule) {
    return (
      <div className="kapacitor-rule-section">
        <h3>Message</h3>
        <textarea ref={(r) => this.message = r} onChange={() => this.handleMessageChange(rule)} />
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

  handleMessageChange(rule) {
    this.props.kapacitorActions.updateMessage(rule.id, this.message.value);
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
