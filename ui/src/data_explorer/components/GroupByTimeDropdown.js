import React, {PropTypes} from 'react'
import classNames from 'classnames'

import groupByTimeOptions from 'hson!../data/groupByTimes.hson'

const {bool, string, func} = PropTypes

const GroupByTimeDropdown = React.createClass({
  propTypes: {
    isOpen: bool,
    selected: string,
    onChooseGroupByTime: func.isRequired,
  },

  render() {
    const {isOpen, selected, onChooseGroupByTime} = this.props

    return (
      <div className="dropdown group-by-time-dropdown">
        <div className="btn btn-sm btn-info dropdown-toggle" data-toggle="dropdown">
          <span className="selected-group-by">{selected || '...'}</span>
          <span className="caret" />
        </div>
        <ul className={classNames("dropdown-menu", {show: isOpen})} aria-labelledby="group-by-dropdown">
          {groupByTimeOptions.map((groupBy) => {
            return (
              <li key={groupBy.menuOption}>
                <a href="#" onClick={() => onChooseGroupByTime(groupBy)}>
                  {groupBy.menuOption}
                </a>
              </li>
            )
          })}
        </ul>
      </div>
    )
  },
})

export default GroupByTimeDropdown
