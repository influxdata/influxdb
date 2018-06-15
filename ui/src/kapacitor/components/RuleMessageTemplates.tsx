import React, {Component} from 'react'

import _ from 'lodash'
import ReactTooltip from 'react-tooltip'

import CodeData from 'src/kapacitor/components/CodeData'

import {RULE_MESSAGE_TEMPLATES} from 'src/kapacitor/constants'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {RuleMessage} from 'src/types/kapacitor'
import {AlertRule} from 'src/types'

interface Props {
  rule: AlertRule
  updateMessage: (id: string, message: string) => void
}

// needs to be React Component for CodeData click handler to work
@ErrorHandling
class RuleMessageTemplates extends Component<Props> {
  constructor(props) {
    super(props)
  }

  public render() {
    return (
      <div className="rule-section--row rule-section--row-last">
        <p>Templates:</p>
        {_.map(RULE_MESSAGE_TEMPLATES, (template, key) => {
          return (
            <CodeData
              key={key}
              template={template}
              onClickTemplate={this.handleClickTemplate(template)}
            />
          )
        })}
        <ReactTooltip
          effect="solid"
          html={true}
          class="influx-tooltip kapacitor-tooltip"
        />
      </div>
    )
  }

  private handleClickTemplate = (template: RuleMessage) => () => {
    const {updateMessage, rule} = this.props
    updateMessage(rule.id, `${rule.message} ${template.label}`)
  }
}

export default RuleMessageTemplates
