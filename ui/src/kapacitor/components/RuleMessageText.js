import React, {Component, PropTypes} from 'react'

class RuleMessageText extends Component {
  constructor(props) {
    super(props)
  }

  handleChange = e => {
    const {rule, updateMessage} = this.props
    updateMessage(rule, e.targe.value)
  }

  render() {
    const {rule} = this.props

    return (
      <textarea
        className="form-control form-malachite monotype rule-builder--message"
        onChange={this.handleChange}
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
