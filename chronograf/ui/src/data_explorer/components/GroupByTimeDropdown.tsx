import React, {SFC} from 'react'
import {withRouter} from 'react-router'
import {Location} from 'history'

import groupByTimeOptions from 'src/data_explorer/data/groupByTimes'

import Dropdown from 'src/shared/components/Dropdown'

import {AUTO_GROUP_BY} from 'src/shared/constants'
import {GroupBy} from 'src/types'

interface GroupByTimeOption {
  defaultTimeBound: string
  seconds: number
  menuOption: string
}

interface Props {
  location?: Location
  selected: string
  onChooseGroupByTime: (groupBy: GroupBy) => void
  isDisabled: boolean
}

const isInRuleBuilder = (pathname: string): boolean =>
  pathname.includes('alert-rules')

const getOptions = (pathname: string): GroupByTimeOption[] =>
  isInRuleBuilder(pathname)
    ? groupByTimeOptions.filter(({menuOption}) => menuOption !== AUTO_GROUP_BY)
    : groupByTimeOptions

const GroupByTimeDropdown: SFC<Props> = ({
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

export default withRouter(GroupByTimeDropdown)
