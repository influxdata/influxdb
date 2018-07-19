import React, {SFC} from 'react'

import {RuleMessage} from 'src/types/kapacitor'

interface Props {
  onClickTemplate: () => void
  template: RuleMessage
}

const CodeData: SFC<Props> = ({onClickTemplate, template}) => (
  <code
    className="rule-builder--message-template"
    data-tip={template.text}
    onClick={onClickTemplate}
  >
    {template.label}
  </code>
)

export default CodeData
