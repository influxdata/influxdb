import React, {SFC, ChangeEvent} from 'react'

import {AlertRule} from 'src/types'

interface Props {
  rule: AlertRule
  updateMessage: (e: ChangeEvent<HTMLTextAreaElement>) => void
}

const RuleMessageText: SFC<Props> = ({rule, updateMessage}) => (
  <div className="rule-builder--message">
    <textarea
      className="form-control input-sm form-malachite monotype"
      onChange={updateMessage}
      placeholder="Example: {{ .ID }} is {{ .Level }} value: {{ index .Fields &quot;value&quot; }}"
      value={rule.message}
      spellCheck={false}
    />
  </div>
)

export default RuleMessageText
