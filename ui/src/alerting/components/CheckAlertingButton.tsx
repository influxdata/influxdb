// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {
  ComponentSize,
  Radio,
  Popover,
  PopoverInteraction,
  PopoverPosition,
  ComponentColor,
  PopoverType,
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

  const isQueryAlertable = isDraftQueryAlertable(draftQueries)

  return (
    <Popover
      visible={!isQueryAlertable}
      position={PopoverPosition.ToTheRight}
      showEvent={PopoverInteraction.None}
      hideEvent={PopoverInteraction.None}
      color={ComponentColor.Primary}
      type={PopoverType.Outline}
      contents={onHide => (
        <div
          style={{
            padding: '8px',
            display: 'flex',
            alignItems: 'left',
            justifyContent: 'center',
            fontSize: '13px',
          }}
        >
          A query that can be used for a check must have
          <br />a field and an aggregate function selection
          <Popover.DismissButton onClick={onHide} />
        </div>
      )}
    >
      <Radio size={ComponentSize.Medium}>
        <Radio.Button
          key="queries"
          id="queries"
          titleText="queries"
          value="queries"
          active={activeTab === 'queries'}
          onClick={handleClick('queries')}
        >
          Query
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
          Check
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
