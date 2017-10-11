import React, {PropTypes, Component} from 'react'
import classnames from 'classnames'

import FunctionSelector from 'shared/components/FunctionSelector'
import {numFunctions, firstFieldName, functionNames} from 'utils/fields'

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
    const {onToggleField, fieldFunc} = this.props
    onToggleField(fieldFunc)
    this.close()
  }

  handleApplyFunctions = selectedFuncs => {
    const {onApplyFuncsToField, fieldFunc} = this.props
    onApplyFuncsToField({
      field: fieldFunc.field,
      funcs: selectedFuncs,
    })
    this.close()
  }

  render() {
    const {isKapacitorRule, fieldFunc, isSelected} = this.props
    const {isOpen} = this.state
    const fieldName = firstFieldName(fieldFunc)
    const funcs = functionNames(fieldFunc)

    let fieldFuncsLabel
    const num = numFunctions(fieldFunc)
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
      <div key={fieldName}>
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
  fieldFunc: arrayOf(
    shape({
      type: string.isRequired,
      name: string.isRequired,
      alias: string,
      args: arrayOf(
        shape({
          type: string.isRequired,
          name: string.isRequired,
        })
      ),
    })
  ).isRequired,
  isSelected: bool.isRequired,
  onToggleField: func.isRequired,
  onApplyFuncsToField: func.isRequired,
  isKapacitorRule: bool.isRequired,
}
export default FieldListItem
