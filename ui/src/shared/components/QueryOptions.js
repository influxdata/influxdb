import React, {PropTypes} from 'react'
import GroupByTimeDropdown from 'src/data_explorer/components/GroupByTimeDropdown'
import FillQuery from 'shared/components/FillQuery'

const QueryOptions = ({
  fill,
  onFill,
  groupBy,
  onGroupByTime,
  isKapacitorRule,
}) =>
  <div className="query-builder--groupby-fill-container">
    <GroupByTimeDropdown
      selected={groupBy.time}
      onChooseGroupByTime={onGroupByTime}
    />
    {isKapacitorRule ? null : <FillQuery value={fill} onChooseFill={onFill} />}
  </div>

const {bool, func, shape, string} = PropTypes

QueryOptions.propTypes = {
  fill: string,
  onFill: func.isRequired,
  groupBy: shape({
    time: string,
  }).isRequired,
  onGroupByTime: func.isRequired,
  isKapacitorRule: bool.isRequired,
}

export default QueryOptions
