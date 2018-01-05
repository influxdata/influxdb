import React, {Component, PropTypes} from 'react'
import {DEFAULT_RULE_ID} from 'src/kapacitor/constants'

class NameSection extends Component {
  constructor(props) {
    super(props)

    this.state = {
      reset: false,
    }
  }

  handleInputBlur = reset => e => {
    const {defaultName, onRuleRename, rule} = this.props

    onRuleRename(rule.id, reset ? defaultName : e.target.value)
    this.setState({reset: false})
  }

  handleKeyDown = e => {
    if (e.key === 'Enter') {
      this.inputRef.blur()
    }
    if (e.key === 'Escape') {
      this.inputRef.value = this.props.defaultName
      this.setState({reset: true}, () => this.inputRef.blur())
    }
  }

  render() {
    const {rule, defaultName} = this.props
    const {reset} = this.state

    return (
      <div className="rule-section">
        <h3 className="rule-section--heading">
          {rule.id === DEFAULT_RULE_ID ? 'Name this Alert Rule' : 'Name'}
        </h3>
        <div className="rule-section--body">
          <div className="rule-section--row rule-section--row-first rule-section--row-last">
            <input
              type="text"
              className="form-control input-md form-malachite"
              defaultValue={defaultName}
              onBlur={this.handleInputBlur(reset)}
              onKeyDown={this.handleKeyDown}
              placeholder="ex: Ruley McRuleface"
              ref={r => (this.inputRef = r)}
            />
          </div>
        </div>
      </div>
    )
  }
}

const {func, string, shape} = PropTypes

NameSection.propTypes = {
  defaultName: string.isRequired,
  onRuleRename: func.isRequired,
  rule: shape({}).isRequired,
}

export default NameSection
