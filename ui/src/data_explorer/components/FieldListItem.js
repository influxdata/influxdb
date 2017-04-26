import React, {PropTypes} from 'react'
import classNames from 'classnames'
import _ from 'lodash'

import MultiSelectDropdown from 'src/shared/components/MultiSelectDropdown'
import Dropdown from 'src/shared/components/Dropdown'

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

  handleToggleField() {
    this.props.onToggleField(this.props.fieldFunc)
  },

  handleApplyFunctions(selectedFuncs) {
    this.props.onApplyFuncsToField({
      field: this.props.fieldFunc.field,
      funcs: this.props.isKapacitorRule ? [selectedFuncs.text] : selectedFuncs,
    })
  },

  render() {
    const {isKapacitorRule, fieldFunc, isSelected} = this.props
    const {field: fieldText} = fieldFunc
    const items = INFLUXQL_FUNCTIONS.map((text) => {
      return {text}
    })

    return (
      <div className={classNames('query-builder--list-item', {active: isSelected})} key={fieldFunc} onClick={_.wrap(fieldFunc, this.handleToggleField)}>
        <span>
          <div className="query-builder--checkbox"></div>
          {fieldText}
        </span>
        {
          isKapacitorRule ?
            <Dropdown items={items} onChoose={this.handleApplyFunctions} selected={fieldFunc.funcs.length ? fieldFunc.funcs[0] : 'Function'} /> :
            <MultiSelectDropdown items={INFLUXQL_FUNCTIONS} onApply={this.handleApplyFunctions} selectedItems={fieldFunc.funcs || []} />
        }
      </div>
    )
  },
})

export default FieldListItem
