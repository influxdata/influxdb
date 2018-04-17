import React, {Component} from 'react'
import PropTypes from 'prop-types'
import _ from 'lodash'
import ReactTooltip from 'react-tooltip'

import CodeData from 'src/kapacitor/components/CodeData'

import {RULE_MESSAGE_TEMPLATES} from 'src/kapacitor/constants'
import {ErrorHandling} from 'src/shared/decorators/errors'

// needs to be React Component for CodeData click handler to work
@ErrorHandling
class RuleMessageTemplates extends Component {
  constructor(props) {
    super(props)
  }

  handleClickTemplate = template => () => {
    const {updateMessage, rule} = this.props
    updateMessage(rule.id, `${rule.message} ${template.label}`)
  }

  render() {
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
}

const {func, shape} = PropTypes

RuleMessageTemplates.propTypes = {
  rule: shape().isRequired,
  updateMessage: func.isRequired,
}

export default RuleMessageTemplates
