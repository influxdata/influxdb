// Libraries
import React, {useState, FunctionComponent} from 'react'
import {connect} from 'react-redux'
import classnames from 'classnames'

// Components
import {DraggableResizer, Stack} from 'src/clockface'
import TimeMachineQueries from 'src/timeMachine/components/Queries'
import TimeMachineVis from 'src/timeMachine/components/Vis'
import ViewOptions from 'src/timeMachine/components/view_options/ViewOptions'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Types
import {TimeMachineTab} from 'src/types/v2/timeMachine'
import {AppState} from 'src/types/v2'

// Styles
import 'src/timeMachine/components/TimeMachine.scss'

const INITIAL_RESIZER_HANDLE = 0.5

interface StateProps {
  activeTab: TimeMachineTab
}

const TimeMachine: FunctionComponent<StateProps> = ({activeTab}) => {
  const [dragPosition, setDragPosition] = useState([INITIAL_RESIZER_HANDLE])

  const containerClassName = classnames('time-machine', {
    'time-machine--split': activeTab === TimeMachineTab.Visualization,
  })

  return (
    <>
      <div className={containerClassName}>
        <DraggableResizer
          stackPanels={Stack.Rows}
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
                <TimeMachineQueries />
              </div>
            </div>
          </DraggableResizer.Panel>
        </DraggableResizer>
      </div>
      {activeTab === TimeMachineTab.Visualization && <ViewOptions />}
    </>
  )
}

const mstp = (state: AppState) => {
  const {activeTab} = getActiveTimeMachine(state)

  return {activeTab}
}

export default connect<StateProps>(mstp)(TimeMachine)
