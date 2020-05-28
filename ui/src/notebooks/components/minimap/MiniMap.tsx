// Libraries
import React, {FC, useContext} from 'react'

// Contexts
import {AppSettingContext} from 'src/notebooks/context/app'
import {NotebookContext, PipeMeta} from 'src/notebooks/context/notebook'

// Components
import MiniMapItem from 'src/notebooks/components/minimap/MiniMapItem'

// Styles
import 'src/notebooks/components/minimap/MiniMap.scss'

const MiniMap: FC = () => {
  const {miniMapVisibility} = useContext(AppSettingContext)
  const {meta, scrollToPipe, updateMeta} = useContext(NotebookContext)

  if (!miniMapVisibility) {
    return null
  }

  const handleClick = (idx: number): void => {
    scrollToPipe(idx)
    updateMeta(idx, {focus: true} as PipeMeta)
  }

  const pipes = meta.map((pipe, index) => (
    <MiniMapItem
      key={`minimap-${pipe.title}-${index}`}
      title={pipe.title}
      focus={pipe.focus}
      index={index}
      onClick={handleClick}
    />
  ))

  return <div className="notebook-minimap">{pipes}</div>
}

export default MiniMap
