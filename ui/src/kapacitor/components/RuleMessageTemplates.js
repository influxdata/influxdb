import React, {Component, PropTypes} from 'react'
import ReactTooltip from 'react-tooltip'

import CodeData from 'src/kapacitor/components/CodeData'

// needs to be React Component for CodeData click handler to work
class RuleMessageTemplates extends Component {
  constructor(props) {
    super(props)
  }

  render() {
    const {rule, actions, templates} = this.props

    return (
      <div className="rule-section--row rule-section--row-last rule-section--border-top">
        <p>Templates:</p>
        {Object.keys(templates).map(t => {
          return (
            <CodeData
              key={t}
              template={templates[t]}
              onClickTemplate={() =>
                actions.updateMessage(
                  rule.id,
                  `${rule.message} ${templates[t].label}`
                )}
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

const {shape} = PropTypes

RuleMessageTemplates.propTypes = {
  rule: shape().isRequired,
  actions: shape().isRequired,
  templates: shape().isRequired,
}

export default RuleMessageTemplates
