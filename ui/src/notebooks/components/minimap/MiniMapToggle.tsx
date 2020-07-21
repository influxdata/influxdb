// Libraries
import React, {FC} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Components
import {Icon, IconFont} from '@influxdata/clockface'

// Actions
import {setNotebookMiniMapState} from 'src/shared/actions/app'

// Utils
import {event} from 'src/cloud/utils/reporting'

// Types
import {AppState} from 'src/types'

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps

const MiniMapToggle: FC<Props> = ({
  notebookMiniMapState,
  handleSetNotebookMiniMapState,
}) => {
  const active = notebookMiniMapState === 'expanded'

  const handleChange = (): void => {
    event('Notebook Toggled Table of Contents', {
      state: active ? 'collapsed' : 'expanded',
    })

    if (active) {
      handleSetNotebookMiniMapState('collapsed')
    } else {
      handleSetNotebookMiniMapState('expanded')
    }
  }

  const glyph = active ? IconFont.Minimize : IconFont.Maximize
  const title = active
    ? 'Click to minimize Table of Contents'
    : 'Click to maximize Table of Contents'

  const headerClassName = `notebook-minimap--header flows-toc-${
    active ? 'collapse' : 'expand'
  }`

  return (
    <button className={headerClassName} onClick={handleChange} title={title}>
      {active && <h6 className="notebook-minimap--title">Table of Contents</h6>}
      <Icon className="notebook-minimap--icon" glyph={glyph} />
    </button>
  )
}

const mstp = (state: AppState) => {
  const {
    app: {
      persisted: {notebookMiniMapState},
    },
  } = state

  return {
    notebookMiniMapState,
  }
}

const mdtp = {
  handleSetNotebookMiniMapState: setNotebookMiniMapState,
}

const connector = connect(mstp, mdtp)

export default connector(MiniMapToggle)
