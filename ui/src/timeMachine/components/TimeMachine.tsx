// Libraries
import React, {useState, FunctionComponent} from 'react'
import {connect} from 'react-redux'
import classnames from 'classnames'

// Components
import {DraggableResizer, Orientation} from '@influxdata/clockface'
import TimeMachineQueries from 'src/timeMachine/components/Queries'
import TimeMachineAlerting from 'src/timeMachine/components/TimeMachineAlerting'
import TimeMachineVis from 'src/timeMachine/components/Vis'
import ViewOptions from 'src/timeMachine/components/view_options/ViewOptions'
import TimeMachineCheckQuery from 'src/timeMachine/components/TimeMachineCheckQuery'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Types
import {AppState, TimeMachineTab} from 'src/types'

const INITIAL_RESIZER_HANDLE = 0.5

interface StateProps {
  activeTab: TimeMachineTab
  isViewingVisOptions: boolean
}

const TimeMachine: FunctionComponent<StateProps> = ({
  activeTab,
  isViewingVisOptions,
}) => {
  const [dragPosition, setDragPosition] = useState([INITIAL_RESIZER_HANDLE])

  const containerClassName = classnames('time-machine', {
    'time-machine--split': isViewingVisOptions,
  })

  let bottomContents: JSX.Element = null

  if (activeTab === 'alerting') {
    bottomContents = <TimeMachineAlerting />
  } else if (activeTab === 'queries') {
    bottomContents = <TimeMachineQueries />
  } else if (activeTab === 'customCheckQuery') {
    bottomContents = <TimeMachineCheckQuery />
  }

  return (
    <>
      {isViewingVisOptions && <ViewOptions />}
      <div className={containerClassName}>
        <DraggableResizer
          handleOrientation={Orientation.Horizontal}
          handlePositions={dragPosition}
          onChangePositions={setDragPosition}
        >
          <DraggableResizer.Panel>
            <div className="time-machine--top">
              <TimeMachineVis />
            </div>
          </DraggableResizer.Panel>
          <DraggableResizer.Panel>
            <div
              className="time-machine--bottom"
              data-testid="time-machine--bottom"
            >
              <div className="time-machine--bottom-contents">
                {bottomContents}
              </div>
            </div>
          </DraggableResizer.Panel>
        </DraggableResizer>
      </div>
    </>
  )
}

const mstp = (state: AppState) => {
  const {activeTab, isViewingVisOptions} = getActiveTimeMachine(state)

  return {activeTab, isViewingVisOptions}
}

export default connect<StateProps>(mstp)(TimeMachine)
