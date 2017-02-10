import React, {PropTypes} from 'react';
import Dropdown from 'shared/components/Dropdown';
import ReactTooltip from 'react-tooltip';
import _ from 'lodash';
import {RULE_MESSAGE_TEMPLATES as templates} from '../constants/index.js';

const DEFAULT_ALERTS = ['http', 'tcp'];
const DEFAULT_ALERT_PLACEHOLDERS = {
  http: 'URL',
  tcp: 'Address',
};
const ALERT_NODES_ACCESSORS = {
  http: 'alertNodes[0].args[0]',
  tcp: 'alertNodes[0].args[0]',
};

const {
  arrayOf,
  func,
  shape,
  string,
} = PropTypes;

export const RuleMessage = React.createClass({
  propTypes: {
    rule: shape({}).isRequired,
    actions: shape({
      updateMessage: func.isRequired,
      updateDetails: func.isRequired,
    }).isRequired,
    enabledAlerts: arrayOf(string.isRequired).isRequired,
  },

  getInitialState() {
    return {
      selectedAlert: null,
      selectedAlertProperty: null,
    };
  },

  handleChangeMessage() {
    const {actions, rule} = this.props;
    actions.updateMessage(rule.id, this.message.value);
  },

  handleChooseAlert(item) {
    const {actions} = this.props;
    actions.updateAlerts(item.ruleID, [item.text]);
    actions.updateAlertNodes(item.ruleID, item.text, '');
    this.setState({selectedAlert: item.text});
  },

  render() {
    const {rule, actions, enabledAlerts} = this.props;
    const defaultAlertEndpoints = DEFAULT_ALERTS.map((text) => {
      return {text, ruleID: rule.id};
    });

    const alerts = enabledAlerts.map((text) => {
      return {text, ruleID: rule.id};
    }).concat(defaultAlertEndpoints);

    const selectedAlert = rule.alerts[0];

    return (
      <div className="kapacitor-rule-section">
        <h3 className="rule-section-heading">Alert Message</h3>
        <div className="rule-section-body">
          <textarea
            className="alert-text message"
            ref={(r) => this.message = r}
            onChange={() => actions.updateMessage(rule.id, this.message.value)}
            placeholder='Example: {{ .ID }} is {{ .Level }} value: {{ index .Fields "value" }}'
            value={rule.message}
          />
          <div className="alert-message--formatting">
            <p>Templates:</p>
            {
              Object.keys(templates).map(t => {
                return (
                <CodeData
                  key={t}
                  template={templates[t]}
                  onClickTemplate={() => actions.updateMessage(rule.id, `${this.message.value} ${templates[t].label}`)}
                />
                );
              })
            }
            <ReactTooltip effect="solid" html={true} offset={{top: -4}} class="influx-tooltip kapacitor-tooltip" />
          </div>
          {
            selectedAlert === 'smtp' ?
            <textarea
              className="alert-text details"
              placeholder="Put email body text here"
              ref={(r) => this.details = r}
              onChange={() => actions.updateDetails(rule.id, this.details.value)}
              value={rule.details}
            /> : null
          }
          <div className="rule-section--item bottom alert-message--endpoint">
            <p>Send this Alert to:</p>
            <Dropdown className="size-256 dropdown-kapacitor" selected={selectedAlert || 'Choose an output'} items={alerts} onChoose={this.handleChooseAlert} />
            {this.renderInput(actions.updateAlertNodes, selectedAlert, rule)}
          </div>
        </div>
      </div>
    );
  },

  renderInput(updateAlertNodes, alert, rule) {
    if (!DEFAULT_ALERTS.find((a) => a === alert)) {
      return null;
    }
    return (<input
      className="form-control col-xs-6"
      type="text"
      placeholder={DEFAULT_ALERT_PLACEHOLDERS[alert]}
      ref={(r) => this.selectedAlertProperty = r}
      onChange={() => updateAlertNodes(rule.id, alert, this.selectedAlertProperty.value)}
      value={_.get(rule, ALERT_NODES_ACCESSORS[alert], '')}
    />);
  },
});

const CodeData = React.createClass({
  propTypes: {
    onClickTemplate: func,
    template: shape({
      label: string,
      text: string,
    }),
  },

  render() {
    const {onClickTemplate, template} = this.props;
    const {label, text} = template;

    return (
      <code data-tip={text} onClick={onClickTemplate}>{label}</code>
    );
  },
});

export default RuleMessage;
