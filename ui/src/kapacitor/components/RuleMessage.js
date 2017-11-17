import React, {Component, PropTypes} from 'react'

import RuleMessageText from 'src/kapacitor/components/RuleMessageText'
import RuleMessageTemplates from 'src/kapacitor/components/RuleMessageTemplates'

class RuleMessage extends Component {
  constructor(props) {
    super(props)
  }

  handleChangeMessage = e => {
    const {actions, rule} = this.props
    actions.updateMessage(rule.id, e.target.value)
  }

  render() {
    const {rule, actions} = this.props

    return (
      <div className="rule-section">
        <h3 className="rule-section--heading">Message</h3>
        <div className="rule-section--body">
          <RuleMessageText
            rule={rule}
            updateMessage={this.handleChangeMessage}
          />
          <RuleMessageTemplates
            rule={rule}
            updateMessage={actions.updateMessage}
          />
        </div>
      </div>
    )
  }
}

const {func, shape} = PropTypes

RuleMessage.propTypes = {
  rule: shape().isRequired,
  actions: shape({
    updateMessage: func.isRequired,
  }).isRequired,
}

export default RuleMessage
