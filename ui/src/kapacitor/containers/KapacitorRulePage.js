import React, {PropTypes} from 'react';
import {withRouter} from 'react-router';
import {connect} from 'react-redux';
import ReactTooltip from 'react-tooltip';
import _ from 'lodash';
import DataSection from '../components/DataSection';
import ValuesSection from '../components/ValuesSection';
import * as kapacitorActionCreators from '../actions/view';
import * as queryActionCreators from '../../chronograf/actions/view';
import {bindActionCreators} from 'redux';
import RuleHeader from 'src/kapacitor/components/RuleHeader';
import RuleGraph from 'src/kapacitor/components/RuleGraph';
import {getKapacitor, getKapacitorConfig} from 'shared/apis/index';
import Dropdown from 'shared/components/Dropdown';
import {ALERTS, DEFAULT_RULE_ID} from 'src/kapacitor/constants';
import {createRule, editRule} from 'src/kapacitor/apis';

export const KapacitorRulePage = React.createClass({
  propTypes: {
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
  },

  getInitialState() {
    return {
      enabledAlerts: [],
    };
  },

  isEditing() {
    const {params} = this.props;
    return params.ruleID && params.ruleID !== 'new';
  },

  componentDidMount() {
    const {params, source, kapacitorActions, addFlashMessage} = this.props;
    if (this.isEditing()) {
      kapacitorActions.fetchRule(source, params.ruleID);
    } else {
      kapacitorActions.loadDefaultRule();
    }

    getKapacitor(source).then((kapacitor) => {
      getKapacitorConfig(kapacitor).then(({data: {sections}}) => {
        const enabledAlerts = Object.keys(sections).filter((section) => {
          return _.get(sections, [section, 'elements', '0', 'options', 'enabled'], false) && ALERTS.includes(section);
        });
        this.setState({kapacitor, enabledAlerts});
      }).catch(() => {
        addFlashMessage({type: 'error', text: `There was a problem communicating with Kapacitor`});
      }).catch(() => {
        addFlashMessage({type: 'error', text: `We couldn't find a configured Kapacitor for this source`});
      });
    });
  },

  thresholdValueEmpty(rule) {
    return rule.trigger === 'threshold' && rule.values.value === '';
  },

  handleSave() {
    const {rules, params} = this.props;
    if (this.isEditing()) { // If we are editing updated rule if not, create a new one
      this.saveOnEdit(rules[params.ruleID]);
    } else {
      this.saveNew(rules[DEFAULT_RULE_ID]);
    }
  },

  saveOnEdit(rule) {
    const {addFlashMessage, queryConfigs} = this.props;

    if (this.thresholdValueEmpty(rule)) {
      addFlashMessage({type: 'error', text: 'Please add a Treshold value to save'});
      return;
    }

    const updatedRule = Object.assign({}, rule, {
      query: queryConfigs[rule.queryID],
    });

    editRule(updatedRule).then(() => {
      addFlashMessage({type: 'success', text: `Rule successfully updated!`});
    }).catch(() => {
      addFlashMessage({type: 'error', text: `There was a problem updating the rule`});
    });
  },

  saveNew(rule) {
    const {addFlashMessage, router, queryConfigs, source} = this.props;

    if (this.thresholdValueEmpty(rule)) {
      addFlashMessage({type: 'error', text: 'Please add a Treshold value to save'});
      return;
    }

    const newRule = Object.assign({}, rule, {
      query: queryConfigs[rule.queryID],
    });
    delete newRule.queryID;

    createRule(this.state.kapacitor, newRule).then(() => {
      router.push(`/sources/${source.id}/alert-rules`);
      addFlashMessage({type: 'success', text: `Rule successfully created`});
    }).catch(() => {
      addFlashMessage({type: 'error', text: `There was a problem creating the rule`});
    });
  },


  handleChooseAlert(item) {
    this.props.kapacitorActions.updateAlerts(item.ruleID, [item.text]);
  },

  render() {
    const {rules, queryConfigs, params, kapacitorActions, source} = this.props;
    const rule = this.isEditing() ? rules[params.ruleID] : rules[DEFAULT_RULE_ID];
    const query = rule && queryConfigs[rule.queryID];

    if (!query) {
      return <div className="page-spinner"></div>;
    }

    return (
      <div className="kapacitor-rule-page">
        <RuleHeader rule={rule} actions={kapacitorActions} onSave={this.handleSave} />
        <div className="rule-builder-wrapper">
          <div className="container-fluid">
            <div className="row">
              <div className="col-xs-12">
                <div className="rule-builder">
                  {this.renderDataSection(query)}
                  {this.renderValuesSection(rule)}
                  <RuleGraph source={source} query={query} rule={rule} />
                  {this.renderMessageSection(rule)}
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  },

  renderDataSection(query) {
    return (
      <div className="kapacitor-rule-section">
        <h3 className="rule-section-heading">Select a Metric</h3>
        <div className="rule-section-body">
          <DataSection source={this.props.source} query={query} actions={this.props.queryActions} />
        </div>
      </div>
    );
  },

  renderValuesSection(rule) {
    const {chooseTrigger, updateRuleValues} = this.props.kapacitorActions;
    return (
      <div className="kapacitor-rule-section">
        <h3 className="rule-section-heading">Values</h3>
        <div className="rule-section-body">
          <ValuesSection rule={rule} query={this.props.queryConfigs[rule.queryID]} onChooseTrigger={chooseTrigger} onUpdateValues={updateRuleValues} />
        </div>
      </div>
    );
  },

  renderMessageSection(rule) {
    const alerts = this.state.enabledAlerts.map((text) => {
      return {text, ruleID: rule.id};
    });

    return (
      <div className="kapacitor-rule-section">
        <h3 className="rule-section-heading">Alert Message</h3>
        <div className="rule-section-body">
          <textarea className="alert-message" ref={(r) => this.message = r} value={rule.message} onChange={() => this.handleMessageChange(rule)} placeholder="Compose your alert message here"/>
          <div className="alert-message--formatting">
            <p>Templates:</p>
            <code data-tip="The ID of the alert">&#123;&#123; .ID &#125;&#125;</code>
            <code data-tip="Measurement name">&#123;&#123; .Name &#125;&#125;</code>
            <code data-tip="The name of the task">&#123;&#123; .TaskName &#125;&#125;</code>
            <code data-tip="Concatenation of all group-by tags of the form <code>&#91;key=value,&#93;+</code>. If no groupBy is performed equal to literal &quot;nil&quot;">&#123;&#123; .Group &#125;&#125;</code>
            <code data-tip="Map of tags. Use <code>&#123;&#123; index .Tags &quot;key&quot; &#125;&#125;</code> to get a specific tag value">&#123;&#123; .Tags &#125;&#125;</code>
            <code data-tip="Alert Level, one of: <code>INFO</code><code>WARNING</code><code>CRITICAL</code>">&#123;&#123; .Level &#125;&#125;</code>
            <code data-tip="Map of fields. Use <code>&#123;&#123; index .Fields &quot;key&quot; &#125;&#125;</code> to get a specific field value">&#123;&#123; .Fields &#125;&#125;</code>
            <code data-tip="The time of the point that triggered the event">&#123;&#123; .Time &#125;&#125;</code>
            <ReactTooltip effect="solid" html={true} offset={{top: -4}} class="influx-tooltip kapacitor-tooltip" />
          </div>
          <div className="rule-section--item bottom alert-message--endpoint">
            <p>Send this Alert to:</p>
            <Dropdown className="size-256" selected={rule.alerts[0] || 'Choose an Endpoint'} items={alerts} onChoose={this.handleChooseAlert} />
          </div>
        </div>
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

export default connect(mapStateToProps, mapDispatchToProps)(withRouter(KapacitorRulePage));
