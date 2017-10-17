import React, {PropTypes, Component} from 'react'
import classnames from 'classnames'
import _ from 'lodash'

import FunctionSelector from 'shared/components/FunctionSelector'
import {firstFieldName} from 'shared/reducers/helpers/fields'

class FieldListItem extends Component {
  constructor(props) {
    super(props)
    this.state = {
      isOpen: false,
    }
  }

  toggleFunctionsMenu = e => {
    if (e) {
      e.stopPropagation()
    }
    this.setState({isOpen: !this.state.isOpen})
  }

  close = () => {
    this.setState({isOpen: false})
  }

  handleToggleField = () => {
    const {onToggleField} = this.props
    const value = this._getFieldName()

    onToggleField({value, type: 'field'})
    this.close()
  }

  handleApplyFunctions = selectedFuncs => {
    const {onApplyFuncsToField} = this.props
    const fieldName = this._getFieldName()
    const field = {value: fieldName, type: 'field'}

    onApplyFuncsToField({
      field,
      funcs: selectedFuncs.map(this._makeFunc),
    })
    this.close()
  }

  _makeFunc = value => ({
    value,
    type: 'func',
  })

  _getFieldName = () => {
    const {fieldFuncs} = this.props
    const fieldFunc = _.head(fieldFuncs)

    return _.get(fieldFunc, 'type') === 'field'
      ? _.get(fieldFunc, 'value')
      : firstFieldName(_.get(fieldFunc, 'args'))
  }

  render() {
    const {isKapacitorRule, isSelected, funcs} = this.props
    const {isOpen} = this.state
    const fieldName = this._getFieldName()

    let fieldFuncsLabel
    const num = funcs.length
    switch (num) {
      case 0:
        fieldFuncsLabel = '0 Functions'
        break
      case 1:
        fieldFuncsLabel = `${num} Function`
        break
      default:
        fieldFuncsLabel = `${num} Functions`
        break
    }
    return (
      <div>
        <div
          className={classnames('query-builder--list-item', {
            active: isSelected,
          })}
          onClick={this.handleToggleField}
          data-test={`query-builder-list-item-field-${fieldName}`}
        >
          <span>
            <div className="query-builder--checkbox" />
            {fieldName}
          </span>
          {isSelected
            ? <div
                className={classnames('btn btn-xs', {
                  active: isOpen,
                  'btn-default': !num,
                  'btn-primary': num,
                })}
                onClick={this.toggleFunctionsMenu}
                data-test={`query-builder-list-item-function-${fieldName}`}
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
  }
}

const {string, shape, func, arrayOf, bool} = PropTypes

FieldListItem.propTypes = {
  fieldFuncs: arrayOf(
    shape({
      type: string.isRequired,
      value: string.isRequired,
      alias: string,
      args: arrayOf(
        shape({
          type: string.isRequired,
          value: string.isRequired,
        })
      ),
    })
  ).isRequired,
  isSelected: bool.isRequired,
  onToggleField: func.isRequired,
  onApplyFuncsToField: func.isRequired,
  isKapacitorRule: bool.isRequired,
  funcs: arrayOf(string.isRequired).isRequired,
}
export default FieldListItem
