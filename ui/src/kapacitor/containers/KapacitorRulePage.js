import React, {PropTypes} from 'react';
import {withRouter} from 'react-router';
import {connect} from 'react-redux';
import _ from 'lodash';
import DataSection from '../components/DataSection';
import ValuesSection from '../components/ValuesSection';
import * as kapacitorActionCreators from '../actions/view';
import * as queryActionCreators from '../../chronograf/actions/view';
import {bindActionCreators} from 'redux';
import RuleHeader from 'src/kapacitor/components/RuleHeader';
import RuleGraph from 'src/kapacitor/components/RuleGraph';
import RuleMessage from 'src/kapacitor/components/RuleMessage';
import {getKapacitor, getKapacitorConfig} from 'shared/apis/index';
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


  render() {
    const {rules, queryConfigs, params, kapacitorActions, source} = this.props;
    const {chooseTrigger, updateRuleValues} = this.props.kapacitorActions;

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
                  <DataSection source={this.props.source} query={query} actions={this.props.queryActions} />
                  <ValuesSection
                    rule={rule}
                    query={queryConfigs[rule.queryID]}
                    onChooseTrigger={chooseTrigger}
                    onUpdateValues={updateRuleValues}
                  />
                  <RuleGraph source={source} query={query} rule={rule} />
                  <RuleMessage rule={rule} actions={kapacitorActions} enabledAlerts={this.state.enabledAlerts} />
                </div>
              </div>
            </div>
          </div>
        </div>
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

export default connect(mapStateToProps, mapDispatchToProps)(withRouter(KapacitorRulePage));
