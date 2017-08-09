import React, {PropTypes} from 'react'
import classnames from 'classnames'
import _ from 'lodash'

import FunctionSelector from 'shared/components/FunctionSelector'

const {string, shape, func, arrayOf, bool} = PropTypes
const FieldListItem = React.createClass({
  propTypes: {
    fieldFunc: shape({
      field: string.isRequired,
      funcs: arrayOf(string).isRequired,
    }).isRequired,
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
    const {field: fieldText} = fieldFunc

    let fieldFuncsLabel
    if (!fieldFunc.funcs.length) {
      fieldFuncsLabel = '0 Functions'
    } else if (fieldFunc.funcs.length === 1) {
      fieldFuncsLabel = `${fieldFunc.funcs.length} Function`
    } else if (fieldFunc.funcs.length > 1) {
      fieldFuncsLabel = `${fieldFunc.funcs.length} Functions`
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
                  'btn-default': !fieldFunc.funcs.length,
                  'btn-primary': fieldFunc.funcs.length,
                })}
                onClick={this.toggleFunctionsMenu}
              >
                {fieldFuncsLabel}
              </div>
            : null}
        </div>
        {isSelected && isOpen
          ? <FunctionSelector
              onApply={this.handleApplyFunctions}
              selectedItems={fieldFunc.funcs || []}
              singleSelect={isKapacitorRule}
            />
          : null}
      </div>
    )
  },
})

export default FieldListItem
