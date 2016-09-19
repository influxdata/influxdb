import React, {PropTypes} from 'react';
import classNames from 'classnames';
import _ from 'lodash';

import MultiSelectDropdown from './MultiSelectDropdown';

import {INFLUXQL_FUNCTIONS} from '../constants';

const {string, shape, func, arrayOf, bool} = PropTypes;
const FieldListItem = React.createClass({
  propTypes: {
    fieldFunc: shape({
      field: string.isRequired,
      funcs: arrayOf(string).isRequired,
    }).isRequired,
    isSelected: bool.isRequired,
    onToggleField: func.isRequired,
    onApplyFuncsToField: func.isRequired,
  },

  handleToggleField() {
    this.props.onToggleField(this.props.fieldFunc);
  },

  handleApplyFunctions(selectedFuncs) {
    this.props.onApplyFuncsToField({
      field: this.props.fieldFunc.field,
      funcs: selectedFuncs,
    });
  },

  render() {
    const {fieldFunc, isSelected} = this.props;
    const {field: fieldText} = fieldFunc;

    return (
      <li className={classNames("query-editor__list-item query-editor__list-checkbox", {checked: isSelected})} key={fieldFunc} onClick={_.wrap(fieldFunc, this.handleToggleField)}>
        <span className="query-editor__list-checkbox__checkbox">{fieldText}</span>
        <div className="query-editor__hidden-dropdown">
          <MultiSelectDropdown items={INFLUXQL_FUNCTIONS} onApply={this.handleApplyFunctions} selectedItems={fieldFunc.funcs || []} />
        </div>
      </li>
    );
  },
});

export default FieldListItem;
