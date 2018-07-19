import React, {Component, ChangeEvent, KeyboardEvent} from 'react'

import {DEFAULT_RULE_ID} from 'src/kapacitor/constants'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {AlertRule} from 'src/types'

interface Props {
  defaultName: string
  onRuleRename: (id: string, name: string) => void
  rule: AlertRule
}

interface State {
  reset: boolean
}

@ErrorHandling
class NameSection extends Component<Props, State> {
  private inputRef: HTMLInputElement

  constructor(props: Props) {
    super(props)

    this.state = {
      reset: false,
    }
  }

  public handleInputBlur = (reset: boolean) => (
    e: ChangeEvent<HTMLInputElement>
  ): void => {
    const {defaultName, onRuleRename, rule} = this.props

    let ruleName: string
    if (reset) {
      ruleName = defaultName
    } else {
      ruleName = e.target.value
    }
    onRuleRename(rule.id, ruleName)
    this.setState({reset: false})
  }

  public handleKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      this.inputRef.blur()
    }
    if (e.key === 'Escape') {
      this.inputRef.value = this.props.defaultName
      this.setState({reset: true}, () => this.inputRef.blur())
    }
  }

  public render() {
    const {defaultName} = this.props
    const {reset} = this.state

    return (
      <div className="rule-section">
        <h3 className="rule-section--heading">{this.header}</h3>
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

  private get header() {
    const {
      rule: {id},
    } = this.props

    if (id === DEFAULT_RULE_ID) {
      return 'Name this Alert Rule'
    }

    return 'Name'
  }
}

export default NameSection
