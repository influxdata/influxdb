import React, {PropTypes} from 'react'

import groupByTimeOptions from 'hson!src/data_explorer/data/groupByTimes.hson'

import Dropdown from 'shared/components/Dropdown'

import {DEFAULT_DASHBOARD_GROUP_BY_INTERVAL} from 'shared/constants'

const {bool, func, string} = PropTypes

const GroupByTimeDropdown = React.createClass({
  propTypes: {
    selected: string,
    onChooseGroupByTime: func.isRequired,
    isInRuleBuilder: bool,
    isInDataExplorer: bool,
  },

  render() {
    const {
      selected,
      onChooseGroupByTime,
      isInRuleBuilder,
      isInDataExplorer,
    } = this.props

    let validOptions = groupByTimeOptions
    if (isInDataExplorer) {
      validOptions = validOptions.filter(
        ({menuOption}) => menuOption !== DEFAULT_DASHBOARD_GROUP_BY_INTERVAL
      )
    }

    return (
      <Dropdown
        className="dropdown-130"
        menuClass={isInRuleBuilder ? 'dropdown-malachite' : null}
        buttonColor={isInRuleBuilder ? 'btn-default' : 'btn-info'}
        items={validOptions.map(groupBy => ({
          ...groupBy,
          text: groupBy.menuOption,
        }))}
        onChoose={onChooseGroupByTime}
        selected={selected ? `Group by ${selected}` : 'Group by Time'}
      />
    )
  },
})

export default GroupByTimeDropdown
