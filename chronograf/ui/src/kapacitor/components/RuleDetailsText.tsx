import React, {PureComponent} from 'react'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {AlertRule} from 'src/types'

interface Props {
  rule: AlertRule
  updateDetails: (id: string, value: string) => void
}

@ErrorHandling
class RuleDetailsText extends PureComponent<Props> {
  public render() {
    const {rule} = this.props
    return (
      <div className="rule-builder--details">
        <textarea
          className="form-control form-malachite monotype"
          onChange={this.handleUpdateDetails}
          placeholder="Enter the body for your email here. Can contain html"
          value={rule.details}
          spellCheck={false}
        />
      </div>
    )
  }

  private handleUpdateDetails = e => {
    const {rule, updateDetails} = this.props
    updateDetails(rule.id, e.target.value)
  }
}
export default RuleDetailsText
