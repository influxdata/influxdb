import React from 'react'
import PropTypes from 'prop-types'
import {withRouter} from 'react-router'

import groupByTimeOptions from 'src/data_explorer/data/groupByTimes'

import Dropdown from 'shared/components/Dropdown'

import {AUTO_GROUP_BY} from 'shared/constants'

const isInRuleBuilder = pathname => pathname.includes('alert-rules')

const getOptions = pathname =>
  isInRuleBuilder(pathname)
    ? groupByTimeOptions.filter(({menuOption}) => menuOption !== AUTO_GROUP_BY)
    : groupByTimeOptions

const GroupByTimeDropdown = ({
  selected,
  onChooseGroupByTime,
  location: {pathname},
  isDisabled,
}) => (
  <div className="group-by-time">
    <label className="group-by-time--label">Group by:</label>
    <Dropdown
      className="group-by-time--dropdown"
      menuClass={isInRuleBuilder(pathname) ? 'dropdown-malachite' : null}
      buttonColor={isInRuleBuilder(pathname) ? 'btn-default' : 'btn-info'}
      items={getOptions(pathname).map(groupBy => ({
        ...groupBy,
        text: groupBy.menuOption,
      }))}
      onChoose={onChooseGroupByTime}
      selected={selected || 'Time'}
      disabled={isDisabled}
    />
  </div>
)

const {bool, func, string, shape} = PropTypes

GroupByTimeDropdown.propTypes = {
  location: shape({
    pathname: string.isRequired,
  }).isRequired,
  selected: string,
  onChooseGroupByTime: func.isRequired,
  isDisabled: bool,
}

export default withRouter(GroupByTimeDropdown)
