// Libraries
import React, {FC} from 'react'

// Components
import {
  Button,
  ComponentStatus,
  IconFont,
  ComponentSize,
} from '@influxdata/clockface'

interface Props {
  onClickPrev: () => void
  onClickNext: () => void
  disablePrev?: boolean
  disableNext?: boolean
  visible?: boolean
  pageSize: number
  startRow: number
}

const ResultsPagination: FC<Props> = ({
  onClickPrev,
  onClickNext,
  visible,
  disablePrev,
  disableNext,
  pageSize,
  startRow,
}) => {
  if (!visible) {
    return null
  }

  const prevButtonStatus = disablePrev
    ? ComponentStatus.Disabled
    : ComponentStatus.Default

  const nextButtonStatus = disableNext
    ? ComponentStatus.Disabled
    : ComponentStatus.Default

  return (
    <div className="query-results--pagination">
      <span className="query-results--pagination-label">{`Showing Results ${startRow} - ${startRow +
        pageSize}`}</span>
      <Button
        className="query-results--pagination-button"
        text="Previous"
        status={prevButtonStatus}
        icon={IconFont.CaretLeft}
        onClick={onClickPrev}
        size={ComponentSize.ExtraSmall}
      />
      <Button
        className="query-results--pagination-button"
        text="Next"
        status={nextButtonStatus}
        icon={IconFont.CaretRight}
        onClick={onClickNext}
        size={ComponentSize.ExtraSmall}
      />
    </div>
  )
}

export default ResultsPagination
