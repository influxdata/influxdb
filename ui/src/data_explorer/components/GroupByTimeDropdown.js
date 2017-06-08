import React, {PropTypes} from 'react'

import groupByTimeOptions from 'hson!src/data_explorer/data/groupByTimes.hson'

import Dropdown from 'shared/components/Dropdown'

const {bool, func, string} = PropTypes

const GroupByTimeDropdown = React.createClass({
  propTypes: {
    selected: string,
    onChooseGroupByTime: func.isRequired,
    isInRuleBuilder: bool,
  },

  render() {
    const {selected, onChooseGroupByTime, isInRuleBuilder} = this.props

    return (
      <Dropdown
        className="dropdown-130"
        menuClass={isInRuleBuilder ? 'dropdown-malachite' : null}
        buttonColor={isInRuleBuilder ? 'btn-default' : 'btn-info'}
        items={groupByTimeOptions.map(groupBy => ({
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
