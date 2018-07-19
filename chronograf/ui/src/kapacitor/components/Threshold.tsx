import React, {Component, FormEvent, ChangeEvent} from 'react'

import {THRESHOLD_OPERATORS} from 'src/kapacitor/constants'
import Dropdown from 'src/shared/components/Dropdown'
import {getDeep} from 'src/utils/wrappers'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {AlertRule, QueryConfig} from 'src/types'

interface TypeItem {
  type: string
  text: string
}

interface Props {
  rule: AlertRule
  onDropdownChange: (item: TypeItem) => void
  onRuleTypeInputChange: (e: ChangeEvent<HTMLInputElement>) => void
  query: QueryConfig
}

@ErrorHandling
class Threshold extends Component<Props> {
  public render() {
    const {
      rule: {
        values: {operator, value},
      },
      query,
      onDropdownChange,
      onRuleTypeInputChange,
    } = this.props

    return (
      <div className="rule-section--row rule-section--row-first rule-section--border-bottom">
        <p>Send Alert where</p>
        <span className="rule-builder--metric">{this.getField(query)}</span>
        <p>is</p>
        <Dropdown
          className="dropdown-180"
          menuClass="dropdown-malachite"
          items={this.operators}
          selected={operator}
          onChoose={onDropdownChange}
        />
        <form style={{display: 'flex'}} onSubmit={this.noopSubmit}>
          <input
            className="form-control input-sm form-malachite monotype"
            style={{width: '160px', marginLeft: '6px'}}
            type="text"
            name="lower"
            spellCheck={false}
            value={value}
            onChange={onRuleTypeInputChange}
            placeholder={this.firstInputPlaceholder}
          />
          {this.secondInput}
        </form>
      </div>
    )
  }

  private get operators() {
    const type = 'operator'
    return THRESHOLD_OPERATORS.map(text => {
      return {text, type}
    })
  }

  private get isSecondInputRequired() {
    const {rule} = this.props
    const operator = getDeep<string>(rule, 'values.operator', '')

    if (operator === 'inside range' || operator === 'outside range') {
      return true
    }
    return false
  }

  private get firstInputPlaceholder() {
    if (this.isSecondInputRequired) {
      return 'lower'
    }

    return null
  }

  private get secondInput() {
    const {rule, onRuleTypeInputChange} = this.props

    const rangeValue = getDeep<string>(rule, 'values.rangeValue', '')

    if (this.isSecondInputRequired) {
      return (
        <input
          className="form-control input-sm form-malachite monotype"
          name="upper"
          style={{width: '160px'}}
          placeholder="Upper"
          type="text"
          spellCheck={false}
          value={rangeValue}
          onChange={onRuleTypeInputChange}
        />
      )
    }
  }

  private noopSubmit = (e: FormEvent<HTMLElement>) => e.preventDefault()

  private getField = ({fields}: QueryConfig): string => {
    const alias = getDeep<string>(fields, '0.alias', '')

    if (!alias) {
      return getDeep<string>(fields, '0.value', 'Select a Time-Series')
    }

    return alias
  }
}

export default Threshold
