// Libraries
import React, {FunctionComponent} from 'react'

// Components
import {Radio, ButtonShape} from '@influxdata/clockface'

// Actions
import {setActiveTab} from 'src/timeMachine/actions'

// Types
import {TimeMachineTab, DashboardDraftQuery} from 'src/types'

interface Props {
  setActiveTab: typeof setActiveTab
  activeTab: TimeMachineTab
  draftQueries: DashboardDraftQuery[]
}

const CheckAlertingButton: FunctionComponent<Props> = ({
  setActiveTab,
  activeTab,
}) => {
  const handleClick = (nextTab: TimeMachineTab) => () => {
    if (activeTab !== nextTab) {
      setActiveTab(nextTab)
    }
  }

  return (
    <Radio shape={ButtonShape.StretchToFit} style={{width: '300px'}}>
      <Radio.Button
        key="queries"
        id="queries"
        titleText="queries"
        value="queries"
        active={activeTab === 'queries'}
        onClick={handleClick('queries')}
      >
        1. Define Query
      </Radio.Button>

      <Radio.Button
        key="alerting"
        id="alerting"
        testID="checkeo--header alerting-tab"
        titleText="alerting"
        value="alerting"
        active={activeTab === 'alerting'}
        onClick={handleClick('alerting')}
      >
        2. Configure Check
      </Radio.Button>
    </Radio>
  )
}

export default CheckAlertingButton
