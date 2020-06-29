// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'

// Components
import {SlideToggle, InputLabel} from '@influxdata/clockface'

// Actions
import {setNotebookMiniMapState} from 'src/shared/actions/app'

// Utils
import {event} from 'src/notebooks/shared/event'

// Types
import {AppState, NotebookMiniMapState} from 'src/types'

interface StateProps {
  notebookMiniMapState: NotebookMiniMapState
}

interface DispatchProps {
  handleSetNotebookMiniMapState: typeof setNotebookMiniMapState
}

type Props = StateProps & DispatchProps

const MiniMapToggle: FC<Props> = ({
  notebookMiniMapState,
  handleSetNotebookMiniMapState,
}) => {
  const active = notebookMiniMapState === 'expanded'

  const handleChange = (): void => {
    event('Toggled Mini Map', {state: active ? 'collapsed' : 'expanded'})

    if (active) {
      handleSetNotebookMiniMapState('collapsed')
    } else {
      handleSetNotebookMiniMapState('expanded')
    }
  }

  return (
    <>
      <SlideToggle active={active} onChange={handleChange} />
      <InputLabel>Minimap</InputLabel>
    </>
  )
}

const mstp = (state: AppState): StateProps => {
  const {
    app: {
      persisted: {notebookMiniMapState},
    },
  } = state

  return {
    notebookMiniMapState,
  }
}

const mdtp: DispatchProps = {
  handleSetNotebookMiniMapState: setNotebookMiniMapState,
}

export default connect<StateProps, DispatchProps>(mstp, mdtp)(MiniMapToggle)
