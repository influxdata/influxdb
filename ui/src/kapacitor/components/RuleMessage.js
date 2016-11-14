import React, {PropTypes} from 'react';
import Dropdown from 'shared/components/Dropdown';
import ReactTooltip from 'react-tooltip';

export const RuleMessage = React.createClass({
  propTypes: {
    rule: PropTypes.shape({}).isRequired,
    actions: PropTypes.shape({
      updateMessage: PropTypes.func.isRequired,
    }).isRequired,
    enabledAlerts: PropTypes.arrayOf(PropTypes.string.isRequired).isRequired,
  },

  handleChangeMessage() {
    const {actions, rule} = this.props;
    actions.updateMessage(rule.id, this.message.value);
  },

  handleChooseAlert(item) {
    const {actions} = this.props;
    actions.updateAlerts(item.ruleID, [item.text]);
  },

  render() {
    const {rule, actions} = this.props;
    const alerts = this.props.enabledAlerts.map((text) => {
      return {text, ruleID: rule.id};
    });

    return (
      <div className="kapacitor-rule-section">
        <h3 className="rule-section-heading">Alert Message</h3>
        <div className="rule-section-body">
          <textarea
            className="alert-message"
            ref={(r) => this.message = r}
            onChange={() => actions.updateMessage(rule.id, this.message.value)}
            placeholder="Compose your alert message here"
            value={rule.message}
          />
          <div className="alert-message--formatting">
            <p>Templates:</p>
            <code data-tip="The ID of the alert">&#123;&#123;.ID&#125;&#125;</code>
            <code data-tip="Measurement name">&#123;&#123;.Name&#125;&#125;</code>
            <code data-tip="The name of the task">&#123;&#123;.TaskName&#125;&#125;</code>
            <code data-tip="Concatenation of all group-by tags of the form <code>&#91;key=value,&#93;+</code>. If no groupBy is performed equal to literal &quot;nil&quot;">&#123;&#123;.Group&#125;&#125;</code>
            <code data-tip="Map of tags. Use <code>&#123;&#123; index .Tags &quot;key&quot; &#125;&#125;</code> to get a specific tag value">&#123;&#123;.Tags&#125;&#125;</code>
            <code data-tip="Alert Level, one of: <code>INFO</code><code>WARNING</code><code>CRITICAL</code>">&#123;&#123;.Level&#125;&#125;</code>
            <code data-tip="Map of fields. Use <code>&#123;&#123; index .Fields &quot;key&quot; &#125;&#125;</code> to get a specific field value">&#123;&#123;.Fields&#125;&#125;</code>
            <code data-tip="The time of the point that triggered the event">&#123;&#123;.Time&#125;&#125;</code>
            <ReactTooltip effect="solid" html={true} offset={{top: -4}} class="influx-tooltip kapacitor-tooltip" />
          </div>
          <div className="rule-section--item bottom alert-message--endpoint">
            <p>Send this Alert to:</p>
            <Dropdown className="size-256" selected={rule.alerts[0] || 'Choose an output'} items={alerts} onChoose={this.handleChooseAlert} />
          </div>
        </div>
      </div>
    );
  },
});
export default RuleMessage;
