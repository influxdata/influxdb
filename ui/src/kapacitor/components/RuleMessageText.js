import React, {Component, PropTypes} from 'react'

class RuleMessageText extends Component {
  constructor(props) {
    super(props)
  }

  render() {
    const {rule, updateMessage} = this.props

    return (
      <textarea
        className="form-control form-malachite monotype rule-builder--message"
        ref={r => (this.message = r)}
        onChange={() => updateMessage(rule.id, this.message.value)}
        placeholder="Example: {{ .ID }} is {{ .Level }} value: {{ index .Fields &quot;value&quot; }}"
        value={rule.message}
        spellCheck={false}
      />
    )
  }
}

const {func, shape} = PropTypes

RuleMessageText.propTypes = {
  rule: shape().isRequired,
  updateMessage: func.isRequired,
}

export default RuleMessageText
