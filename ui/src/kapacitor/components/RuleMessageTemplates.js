import React, {Component, PropTypes} from 'react'
import _ from 'lodash'
import ReactTooltip from 'react-tooltip'

import CodeData from 'src/kapacitor/components/CodeData'

import {RULE_MESSAGE_TEMPLATES} from 'src/kapacitor/constants'

// needs to be React Component for CodeData click handler to work
class RuleMessageTemplates extends Component {
  constructor(props) {
    super(props)
  }

  render() {
    const {rule, updateMessage} = this.props

    return (
      <div className="rule-section--row rule-section--row-last rule-section--border-top">
        <p>Templates:</p>
        {_.map(RULE_MESSAGE_TEMPLATES, (template, key) => {
          return (
            <CodeData
              key={key}
              template={template}
              onClickTemplate={() =>
                updateMessage(rule.id, `${rule.message} ${template.label}`)}
            />
          )
        })}
        <ReactTooltip
          effect="solid"
          html={true}
          offset={{top: -4}}
          class="influx-tooltip kapacitor-tooltip"
        />
      </div>
    )
  }
}

const {func, shape} = PropTypes

RuleMessageTemplates.propTypes = {
  rule: shape().isRequired,
  updateMessage: func.isRequired,
}

export default RuleMessageTemplates
