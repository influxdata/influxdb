// Libraries
import React, {SFC} from 'react'
import {connect} from 'react-redux'
import {get} from 'lodash'

// Components
import {Dropdown, ComponentSize} from 'src/clockface'

// Actions
import {setQuerySource} from 'src/shared/actions/v2/timeMachines'

// Utils
import {getActiveTimeMachine} from 'src/shared/selectors/timeMachines'
import {getSources} from 'src/sources/selectors'

// Types
import {AppState, Source} from 'src/types/v2'

const DYNAMIC_SOURCE = {id: '', name: 'Dynamic Source'}
const DROPDOWN_WIDTH = 200

interface StateProps {
  sources: Source[]
  selectedSourceID: string | null
}

interface DispatchProps {
  onSelectSource: (sourceID: string) => void
}

type Props = StateProps & DispatchProps

const TimeMachineSourceDropdown: SFC<Props> = props => {
  const {sources, selectedSourceID, onSelectSource} = props

  return (
    <Dropdown
      selectedID={selectedSourceID}
      onChange={onSelectSource}
      buttonSize={ComponentSize.Small}
      widthPixels={DROPDOWN_WIDTH}
    >
      {[DYNAMIC_SOURCE, ...sources].map(source => (
        <Dropdown.Item key={source.id} id={source.id} value={source.id}>
          {source.name}
        </Dropdown.Item>
      ))}
    </Dropdown>
  )
}

const mstp = (state: AppState) => {
  const sources = getSources(state)
  const {activeQueryIndex, view} = getActiveTimeMachine(state)
  const selectedSourceID: string = get(
    view,
    `properties.queries.${activeQueryIndex}.sourceID`,
    ''
  )

  return {sources, selectedSourceID}
}

const mdtp = {
  onSelectSource: setQuerySource,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(TimeMachineSourceDropdown)
