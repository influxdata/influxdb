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
      <div className={classNames("dropdown group-by-time-dropdown", {open: isOpen})}>
        <div className="btn btn-sm btn-info dropdown-toggle" data-toggle="dropdown">
          <span>Group by {selected || 'time'}</span>
          <span className="caret"></span>
        </div>
        <ul className="dropdown-menu">
          {groupByTimeOptions.map((groupBy) => {
            return (
              <li className="dropdown-item" key={groupBy.menuOption}onClick={() => onChooseGroupByTime(groupBy)}>
                <a href="#">
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
