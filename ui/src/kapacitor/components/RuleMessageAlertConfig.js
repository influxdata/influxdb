import React, {PropTypes} from 'react';

import {
  DEFAULT_ALERT_PLACEHOLDERS,
  ALERT_NODES_ACCESSORS,
} from '../constants';

const RuleMessageAlertConfig = ({
  updateAlertNodes,
  alert,
  rule,
}) => {
  if (!Object.keys(DEFAULT_ALERT_PLACEHOLDERS).find((a) => a === alert)) {
    return null;
  }
  return (
    <form className="form-group col-xs-12" style={{marginTop: '8px'}}>
      <div>
        <label htmlFor="alert-input">{DEFAULT_ALERT_PLACEHOLDERS[alert]}</label>
      </div>
      <div>
        <input
          id="alert-input"
          className="form-control"
          type="text"
          placeholder={DEFAULT_ALERT_PLACEHOLDERS[alert]}
          name="alertProperty"
          onChange={(evt) => updateAlertNodes(rule.id, alert, evt.target.form.alertProperty.value)}
          value={ALERT_NODES_ACCESSORS[alert](rule)}
        />
      </div>
    </form>
  );
};

const {
  func,
  shape,
  string,
} = PropTypes;

RuleMessageAlertConfig.propTypes = {
  updateAlertNodes: func.isRequired,
  alert: string,
  rule: shape({}).isRequired,
};

export default RuleMessageAlertConfig;
