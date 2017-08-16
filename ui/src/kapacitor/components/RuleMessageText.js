import React, {PropTypes} from 'react'

const RuleMessageText = ({rule, updateMessage}) =>
  <textarea
    className="form-control form-malachite monotype rule-builder--message"
    onChange={updateMessage}
    placeholder="Example: {{ .ID }} is {{ .Level }} value: {{ index .Fields &quot;value&quot; }}"
    value={rule.message}
    spellCheck={false}
  />

const {func, shape} = PropTypes

RuleMessageText.propTypes = {
  rule: shape().isRequired,
  updateMessage: func.isRequired,
}

export default RuleMessageText
