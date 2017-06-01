import React, {PropTypes} from 'react'
import classnames from 'classnames'
import _ from 'lodash'

import FunctionSelector from 'shared/components/FunctionSelector'
import Dropdown from 'shared/components/Dropdown'

import {INFLUXQL_FUNCTIONS} from '../constants'

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
      funcs: this.props.isKapacitorRule ? [selectedFuncs.text] : selectedFuncs,
    })
    this.setState({isOpen: false})
  },

  render() {
    const {isKapacitorRule, fieldFunc, isSelected} = this.props
    const {isOpen} = this.state
    const {field: fieldText} = fieldFunc
    const items = INFLUXQL_FUNCTIONS.map(text => {
      return {text}
    })

    if (isKapacitorRule) {
      return (
        <div
          className={classnames('query-builder--list-item', {
            active: isSelected,
          })}
          key={fieldFunc}
          onClick={_.wrap(fieldFunc, this.handleToggleField)}
        >
          <span>
            <div className="query-builder--checkbox" />
            {fieldText}
          </span>
          {isSelected
            ? <Dropdown
                className="dropdown-110"
                menuClass="dropdown-malachite"
                buttonSize="btn-xs"
                items={items}
                onChoose={this.handleApplyFunctions}
                selected={
                  fieldFunc.funcs.length ? fieldFunc.funcs[0] : 'Function'
                }
              />
            : null}
        </div>
      )
    }

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
            />
          : null}
      </div>
    )
  },
})

export default FieldListItem
