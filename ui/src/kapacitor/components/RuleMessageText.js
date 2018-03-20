import React from 'react'
import PropTypes from 'prop-types'

const RuleMessageText = ({rule, updateMessage}) =>
  <div className="rule-builder--message">
    <textarea
      className="form-control input-sm form-malachite monotype"
      onChange={updateMessage}
      placeholder="Example: {{ .ID }} is {{ .Level }} value: {{ index .Fields &quot;value&quot; }}"
      value={rule.message}
      spellCheck={false}
    />
  </div>

const {func, shape} = PropTypes

RuleMessageText.propTypes = {
  rule: shape().isRequired,
  updateMessage: func.isRequired,
}

export default RuleMessageText
