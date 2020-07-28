// Libraries
import React, {FunctionComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Components
import {
  SelectGroup,
  ButtonShape,
  ComponentStatus,
  ComponentSize,
} from '@influxdata/clockface'

// Actions
import {
  setWindowPeriodSelectionMode,
  selectAggregateWindow,
} from 'src/timeMachine/actions/queryBuilder'

// Utils
import {
  getActiveQuery,
  getWindowPeriodFromTimeRange,
  getIsInCheckOverlay,
} from 'src/timeMachine/selectors'

// Constants
import {DURATIONS} from 'src/timeMachine/constants/queryBuilder'

// Types
import {AppState} from 'src/types'
import DurationInput from 'src/shared/components/DurationInput'
import {AGG_WINDOW_AUTO} from '../constants/queryBuilder'

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps

const WindowPeriod: FunctionComponent<Props> = ({
  period,
  autoWindowPeriod,
  onSetWindowPeriodSelectionMode,
  onSelectAggregateWindow,
  isInCheckOverlay,
}) => {
  const isAutoWindowPeriod = isInCheckOverlay || period === AGG_WINDOW_AUTO

  let durationDisplay = period

  if (!period || isAutoWindowPeriod) {
    durationDisplay = autoWindowPeriod
      ? `${AGG_WINDOW_AUTO} (${autoWindowPeriod})`
      : AGG_WINDOW_AUTO
  }

  const durationInputStatus = isAutoWindowPeriod
    ? ComponentStatus.Disabled
    : ComponentStatus.Default

  if (isInCheckOverlay) {
    return (
      <DurationInput
        onSubmit={onSelectAggregateWindow}
        value={durationDisplay}
        suggestions={DURATIONS}
        submitInvalid={false}
        status={durationInputStatus}
      />
    )
  }

  return (
    <>
      <SelectGroup
        shape={ButtonShape.StretchToFit}
        size={ComponentSize.ExtraSmall}
      >
        <SelectGroup.Option
          name="custom"
          id="custom-window-period"
          active={!isAutoWindowPeriod}
          value="custom"
          onClick={onSetWindowPeriodSelectionMode}
          titleText="Custom"
        >
          Custom
        </SelectGroup.Option>
        <SelectGroup.Option
          name="auto"
          id="auto-window-period"
          active={isAutoWindowPeriod}
          value="auto"
          onClick={onSetWindowPeriodSelectionMode}
          titleText="Auto"
        >
          Auto
        </SelectGroup.Option>
      </SelectGroup>
      <DurationInput
        onSubmit={onSelectAggregateWindow}
        value={durationDisplay}
        suggestions={DURATIONS}
        submitInvalid={false}
        status={durationInputStatus}
      />
    </>
  )
}

const mstp = (state: AppState) => {
  const {builderConfig} = getActiveQuery(state)
  const {
    aggregateWindow: {period},
  } = builderConfig

  return {
    period,
    isInCheckOverlay: getIsInCheckOverlay(state),
    autoWindowPeriod: getWindowPeriodFromTimeRange(state),
  }
}

const mdtp = {
  onSetWindowPeriodSelectionMode: setWindowPeriodSelectionMode,
  onSelectAggregateWindow: selectAggregateWindow,
}

const connector = connect(mstp, mdtp)

export default connector(WindowPeriod)
