import React from 'react'
import PropTypes from 'prop-types'
import Dropdown from 'shared/components/Dropdown'

const GraphOptionsSortBy = ({sortByOptions, onChooseSortBy}) =>
  <div className="form-group col-xs-6">
    <label>Sort By</label>
    <Dropdown
      items={sortByOptions}
      selected={sortByOptions[0].text}
      buttonColor="btn-default"
      buttonSize="btn-sm"
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
