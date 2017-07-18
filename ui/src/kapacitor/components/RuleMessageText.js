import React, {Component, PropTypes} from 'react'

class RuleMessageText extends Component {
  constructor(props) {
    super(props)
  }

  render() {
    const {rule, actions} = this.props

    return (
      <textarea
        className="form-control form-malachite monotype rule-builder--message"
        ref={r => (this.message = r)}
        onChange={() => actions.updateMessage(rule.id, this.message.value)}
        placeholder="Example: {{ .ID }} is {{ .Level }} value: {{ index .Fields &quot;value&quot; }}"
        value={rule.message}
        spellCheck={false}
      />
    )
  }
}

const {shape} = PropTypes

RuleMessageText.propTypes = {
  rule: shape().isRequired,
  actions: shape().isRequired,
}

export default RuleMessageText
