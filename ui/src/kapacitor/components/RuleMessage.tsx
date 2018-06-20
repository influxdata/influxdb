import React, {Component, ChangeEvent} from 'react'

import RuleMessageText from 'src/kapacitor/components/RuleMessageText'
import RuleMessageTemplates from 'src/kapacitor/components/RuleMessageTemplates'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {AlertRule} from 'src/types'
import {KapacitorRuleActions} from 'src/types/actions'

interface Props {
  rule: AlertRule
  ruleActions: KapacitorRuleActions
}

@ErrorHandling
class RuleMessage extends Component<Props> {
  constructor(props) {
    super(props)
  }

  public render() {
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

  private handleChangeMessage = (e: ChangeEvent<HTMLTextAreaElement>) => {
    const {ruleActions, rule} = this.props
    ruleActions.updateMessage(rule.id, e.target.value)
  }
}

export default RuleMessage
