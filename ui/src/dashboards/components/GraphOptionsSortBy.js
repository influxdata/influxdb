import React, {PropTypes} from 'react'
import Dropdown from 'shared/components/Dropdown'

const GraphOptionsSortBy = ({sortByOptions, onChooseSortBy}) =>
  <div className="form-group col-xs-6">
    <label>Sort By</label>
    <Dropdown
      items={sortByOptions}
      selected={sortByOptions[0].text}
      buttonColor="btn-primary"
      buttonSize="btn-xs"
      className="dropdown-stretch"
      onChoose={onChooseSortBy}
    />
  </div>

const {arrayOf, func, shape, string} = PropTypes

GraphOptionsSortBy.propTypes = {
  sortByOptions: arrayOf(
    shape({
      text: string.isRequired,
    }).isRequired
  ),
  onChooseSortBy: func,
}

export default GraphOptionsSortBy
