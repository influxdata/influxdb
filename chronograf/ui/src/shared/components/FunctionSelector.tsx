import React, {PureComponent, MouseEvent} from 'react'
import classnames from 'classnames'
import _ from 'lodash'
import {INFLUXQL_FUNCTIONS} from 'src/shared/constants/influxql'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  onApply: (item: string[]) => void
  selectedItems: string[]
}

interface State {
  localSelectedItems: string[]
}

@ErrorHandling
class FunctionSelector extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      localSelectedItems: this.props.selectedItems,
    }
  }

  public componentWillUpdate(nextProps) {
    if (!_.isEqual(this.props.selectedItems, nextProps.selectedItems)) {
      this.setState({localSelectedItems: nextProps.selectedItems})
    }
  }

  public render() {
    return (
      <div className="function-selector">
        <div className="function-selector--header">
          <span>{this.headerText}</span>
          <div
            className="btn btn-xs btn-success"
            onClick={this.handleApplyFunctions}
            data-test="function-selector-apply"
          >
            Apply
          </div>
        </div>
        <div className="function-selector--grid">
          {INFLUXQL_FUNCTIONS.map((f, i) => {
            return (
              <div
                key={i}
                className={classnames('function-selector--item', {
                  active: this.isSelected(f),
                })}
                onClick={_.wrap(f, this.onSelect)}
                data-test={`function-selector-item-${f}`}
              >
                {f}
              </div>
            )
          })}
        </div>
      </div>
    )
  }

  private get headerText(): string {
    const numItems = this.state.localSelectedItems.length
    if (!numItems) {
      return 'Select functions below'
    }

    return `${numItems} Selected`
  }

  private onSelect = (item: string, e: MouseEvent<HTMLDivElement>): void => {
    e.stopPropagation()

    const {localSelectedItems} = this.state

    let nextItems
    if (this.isSelected(item)) {
      nextItems = localSelectedItems.filter(i => i !== item)
    } else {
      nextItems = [...localSelectedItems, item]
    }

    this.setState({localSelectedItems: nextItems})
  }

  private isSelected = (item: string): boolean => {
    return !!this.state.localSelectedItems.find(text => text === item)
  }

  private handleApplyFunctions = (e: MouseEvent<HTMLDivElement>): void => {
    e.stopPropagation()
    this.props.onApply(this.state.localSelectedItems)
  }
}

export default FunctionSelector
