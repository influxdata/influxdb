// Libraries
import React, {FC, useContext} from 'react'
import {connect} from 'react-redux'

// Contexts
import {NotebookContext, PipeMeta} from 'src/notebooks/context/notebook'
import {ScrollContext} from 'src/notebooks/context/scroll'

// Components
import {DapperScrollbars} from '@influxdata/clockface'
import MiniMapItem from 'src/notebooks/components/minimap/MiniMapItem'

// Types
import {AppState, NotebookMiniMapState} from 'src/types'

// Styles
import 'src/notebooks/components/minimap/MiniMap.scss'

interface StateProps {
  notebookMiniMapState: NotebookMiniMapState
}

const MiniMap: FC<StateProps> = ({notebookMiniMapState}) => {
  const {meta, updateMeta} = useContext(NotebookContext)
  const {scrollToPipe} = useContext(ScrollContext)

  if (notebookMiniMapState === 'collapsed') {
    return null
  }

  const handleClick = (idx: number): void => {
    const {panelRef} = meta[idx]
    scrollToPipe(panelRef)
    updateMeta(idx, {focus: true} as PipeMeta)
  }

  const pipes = meta.map((pipe, index) => (
    <MiniMapItem
      key={`minimap-${pipe.title}-${index}`}
      title={pipe.title}
      focus={pipe.focus}
      visible={pipe.visible}
      index={index}
      onClick={handleClick}
    />
  ))

  return (
    <DapperScrollbars className="notebook-minimap" autoHide={true}>
      <div className="notebook-minimap--list">{pipes}</div>
    </DapperScrollbars>
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

export default connect<StateProps, {}>(
  mstp,
  null
)(MiniMap)
