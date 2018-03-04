import React, {SFC} from 'react'
import classnames from 'classnames'
import TagList from 'src/shared/components/TagList'

import {Query, Source} from 'src/types'

interface Props {
  query: Query
  querySource: Source
  isActive: boolean
  measurement: string
  numTagsActive: number
  areTagsAccepted: boolean
  onChooseTag: () => void
  onGroupByTag: () => void
  onAcceptReject: () => void
  onChooseMeasurement: (measurement: string) => () => void
}

const noop = () => {}

const MeasurementListItem: SFC<Props> = ({
  query,
  isActive,
  querySource,
  measurement,
  onChooseTag,
  onGroupByTag,
  numTagsActive,
  onAcceptReject,
  areTagsAccepted,
  onChooseMeasurement,
}) =>
  <div
    key={measurement}
    onClick={isActive ? noop : onChooseMeasurement(measurement)}
  >
    <div className={classnames('query-builder--list-item', {active: isActive})}>
      <span>
        <div className="query-builder--caret icon caret-right" />
        {measurement}
      </span>
      {isActive &&
        numTagsActive >= 1 &&
        <div
          className={classnames('flip-toggle', {flipped: areTagsAccepted})}
          onClick={onAcceptReject}
        >
          <div className="flip-toggle--container">
            <div className="flip-toggle--front">!=</div>
            <div className="flip-toggle--back">=</div>
          </div>
        </div>}
    </div>
    {isActive &&
      <TagList
        query={query}
        querySource={querySource}
        onChooseTag={onChooseTag}
        onGroupByTag={onGroupByTag}
      />}
  </div>

export default MeasurementListItem
