// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {
  Radio,
  Popover,
  PopoverInteraction,
  PopoverPosition,
  ComponentColor,
  PopoverType,
  ButtonShape,
  Icon,
  IconFont,
} from '@influxdata/clockface'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'
import {isDraftQueryAlertable} from 'src/timeMachine/utils/queryBuilder'

// Actions
import {setActiveTab} from 'src/timeMachine/actions'

// Types
import {AppState, TimeMachineTab, DashboardDraftQuery} from 'src/types'

interface DispatchProps {
  setActiveTab: typeof setActiveTab
}

interface StateProps {
  activeTab: TimeMachineTab
  draftQueries: DashboardDraftQuery[]
}

type Props = DispatchProps & StateProps

const CheckAlertingButton: FunctionComponent<Props> = ({
  setActiveTab,
  draftQueries,
  activeTab,
}) => {
  const handleClick = (nextTab: TimeMachineTab) => () => {
    if (activeTab !== nextTab) {
      setActiveTab(nextTab)
    }
  }

  const {
    oneQuery,
    builderMode,
    singleAggregateFunc,
    singleField,
  } = isDraftQueryAlertable(draftQueries)

  const isQueryAlertable =
    oneQuery && builderMode && singleAggregateFunc && singleField

  return (
    <Popover
      style={{width: '100%'}}
      visible={!isQueryAlertable}
      position={PopoverPosition.ToTheRight}
      showEvent={PopoverInteraction.None}
      hideEvent={PopoverInteraction.None}
      color={ComponentColor.Secondary}
      type={PopoverType.Outline}
      contents={onHide => (
        <div className="query-checklist--popover">
          <p>In order to define a Check your query must:</p>
          <ul className="query-checklist--list">
            <QueryChecklistItem
              text="Have 1 field selected"
              selected={singleField}
            />
            <QueryChecklistItem
              text="Have 1 aggregate function selected"
              selected={singleAggregateFunc}
            />
          </ul>
          <Popover.DismissButton
            onClick={onHide}
            color={ComponentColor.Secondary}
          />
        </div>
      )}
    >
      <Radio shape={ButtonShape.StretchToFit}>
        <Radio.Button
          key="queries"
          id="queries"
          titleText="queries"
          value="queries"
          active={activeTab === 'queries'}
          onClick={handleClick('queries')}
        >
          1. Query
        </Radio.Button>

        <Radio.Button
          key="alerting"
          id="alerting"
          titleText="alerting"
          value="alerting"
          active={activeTab === 'alerting'}
          onClick={handleClick('alerting')}
          disabled={!isQueryAlertable}
        >
          2. Check
        </Radio.Button>
      </Radio>
    </Popover>
  )
}

const mstp = (state: AppState): StateProps => {
  const {activeTab, draftQueries} = getActiveTimeMachine(state)

  return {activeTab, draftQueries}
}

const mdtp: DispatchProps = {
  setActiveTab: setActiveTab,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(CheckAlertingButton)

interface ChecklistItemProps {
  selected: boolean
  text: string
}

const QueryChecklistItem: FunctionComponent<ChecklistItemProps> = ({
  selected,
  text,
}) => {
  const className = selected
    ? 'query-checklist--item valid'
    : 'query-checklist--item error'
  const icon = selected ? IconFont.Checkmark : IconFont.Remove

  return (
    <li className={className}>
      <Icon glyph={icon} />
      {text}
    </li>
  )
}
