import _ from 'lodash'
import React, {PropTypes} from 'react'
import classnames from 'classnames'

import FunctionSelector from 'shared/components/FunctionSelector'
import {numFunctions, fieldNamesDeep, functionNames} from 'utils/fields'

const {string, shape, func, arrayOf, bool} = PropTypes
const FieldListItem = React.createClass({
  propTypes: {
    fieldFunc: arrayOf(
      shape({
        type: string,
        name: string,
      })
    ).isRequired,
    isSelected: bool.isRequired,
    onToggleField: func.isRequired,
    onApplyFuncsToField: func.isRequired,
    isKapacitorRule: bool.isRequired,
  },

  getInitialState() {
    return {
      isOpen: false,
    }
  },

  toggleFunctionsMenu(e) {
    if (e) {
      e.stopPropagation()
    }
    this.setState({isOpen: !this.state.isOpen})
  },

  handleToggleField() {
    this.props.onToggleField(this.props.fieldFunc)
    this.setState({isOpen: false})
  },

  handleApplyFunctions(selectedFuncs) {
    this.props.onApplyFuncsToField({
      field: this.props.fieldFunc.field,
      funcs: selectedFuncs,
    })
    this.setState({isOpen: false})
  },

  render() {
    const {isKapacitorRule, fieldFunc, isSelected} = this.props
    const {isOpen} = this.state
    const fieldText = _.head(fieldNamesDeep(fieldFunc))
    const funcs = functionNames(fieldFunc)

    let fieldFuncsLabel
    const num = numFunctions(fieldFunc)
    switch (num) {
      case 0:
        fieldFuncsLabel = '0 Functions'
      case 1:
        fieldFuncsLabel = `${num} Function`
      default:
        fieldFuncsLabel = `${num} Functions`
    }
    return (
      <div key={fieldFunc}>
        <div
          className={classnames('query-builder--list-item', {
            active: isSelected,
          })}
          onClick={_.wrap(fieldFunc, this.handleToggleField)}
          data-test={`query-builder-list-item-field-${fieldText}`}
        >
          <span>
            <div className="query-builder--checkbox" />
            {fieldText}
          </span>
          {isSelected
            ? <div
                className={classnames('btn btn-xs', {
                  active: isOpen,
                  'btn-default': !num,
                  'btn-primary': num,
                })}
                onClick={this.toggleFunctionsMenu}
                data-test={`query-builder-list-item-function-${fieldText}`}
              >
                {fieldFuncsLabel}
              </div>
            : null}
        </div>
        {isSelected && isOpen
          ? <FunctionSelector
              onApply={this.handleApplyFunctions}
              selectedItems={funcs}
              singleSelect={isKapacitorRule}
            />
          : null}
      </div>
    )
  },
})

export default FieldListItem
