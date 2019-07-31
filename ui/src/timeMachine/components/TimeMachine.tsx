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

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Types
import {AppState, TimeMachineTab} from 'src/types'
import {TimeMachineIDs} from 'src/timeMachine/constants'

const INITIAL_RESIZER_HANDLE = 0.5

interface StateProps {
  activeTab: TimeMachineTab
  activeTimeMachineID: TimeMachineIDs
}

const TimeMachine: FunctionComponent<StateProps> = ({
  activeTimeMachineID,
  activeTab,
}) => {
  const [dragPosition, setDragPosition] = useState([INITIAL_RESIZER_HANDLE])

  const containerClassName = classnames('time-machine', {
    'time-machine--split': activeTab === 'visualization',
  })

  return (
    <>
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
                {activeTab === 'alerting' ? (
                  <TimeMachineAlerting
                    activeTimeMachineID={activeTimeMachineID}
                  />
                ) : (
                  <TimeMachineQueries />
                )}
              </div>
            </div>
          </DraggableResizer.Panel>
        </DraggableResizer>
      </div>
      {activeTab === 'visualization' && <ViewOptions />}
    </>
  )
}

const mstp = (state: AppState) => {
  const {activeTab} = getActiveTimeMachine(state)
  const {activeTimeMachineID} = state.timeMachines

  return {activeTab, activeTimeMachineID}
}

export default connect<StateProps>(mstp)(TimeMachine)
