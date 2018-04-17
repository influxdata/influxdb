import React, {Component} from 'react'
import PropTypes from 'prop-types'

import RuleMessageText from 'src/kapacitor/components/RuleMessageText'
import RuleMessageTemplates from 'src/kapacitor/components/RuleMessageTemplates'
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
class RuleMessage extends Component {
  constructor(props) {
    super(props)
  }

  handleChangeMessage = e => {
    const {ruleActions, rule} = this.props
    ruleActions.updateMessage(rule.id, e.target.value)
  }

  render() {
    const {rule, ruleActions} = this.props

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
            updateMessage={ruleActions.updateMessage}
          />
        </div>
      </div>
    )
  }
}

const {func, shape} = PropTypes

RuleMessage.propTypes = {
  rule: shape().isRequired,
  ruleActions: shape({
    updateMessage: func.isRequired,
  }).isRequired,
}

export default RuleMessage
