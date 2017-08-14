import React, {Component, PropTypes} from 'react'
import classnames from 'classnames'
import _ from 'lodash'
import {INFLUXQL_FUNCTIONS} from 'src/data_explorer/constants'

class FunctionSelector extends Component {
  constructor(props) {
    super(props)

    this.state = {
      localSelectedItems: this.props.selectedItems,
    }

    this.onSelect = ::this.onSelect
    this.onSingleSelect = ::this.onSingleSelect
    this.handleApplyFunctions = ::this.handleApplyFunctions
  }

  componentWillUpdate(nextProps) {
    if (!_.isEqual(this.props.selectedItems, nextProps.selectedItems)) {
      this.setState({localSelectedItems: nextProps.selectedItems})
    }
  }

  onSelect(item, e) {
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

  onSingleSelect(item) {
    if (item === this.state.localSelectedItems[0]) {
      this.props.onApply([])
      this.setState({localSelectedItems: []})
    } else {
      this.props.onApply([item])
      this.setState({localSelectedItems: [item]})
    }
  }

  isSelected(item) {
    return !!this.state.localSelectedItems.find(text => text === item)
  }

  handleApplyFunctions(e) {
    e.stopPropagation()

    this.props.onApply(this.state.localSelectedItems)
  }

  render() {
    const {localSelectedItems} = this.state
    const {singleSelect} = this.props

    return (
      <div className="function-selector">
        {singleSelect
          ? null
          : <div className="function-selector--header">
              <span>
                {localSelectedItems.length > 0
                  ? `${localSelectedItems.length} Selected`
                  : 'Select functions below'}
              </span>
              <div
                className="btn btn-xs btn-success"
                onClick={this.handleApplyFunctions}
                data-test="function-selector-apply"
              >
                Apply
              </div>
            </div>}
        <div className="function-selector--grid">
          {INFLUXQL_FUNCTIONS.map((f, i) => {
            return (
              <div
                key={i}
                className={classnames('function-selector--item', {
                  active: this.isSelected(f),
                })}
                onClick={_.wrap(
                  f,
                  singleSelect ? this.onSingleSelect : this.onSelect
                )}
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
}

const {arrayOf, bool, func, string} = PropTypes

FunctionSelector.propTypes = {
  onApply: func.isRequired,
  selectedItems: arrayOf(string.isRequired).isRequired,
  singleSelect: bool,
}

export default FunctionSelector
