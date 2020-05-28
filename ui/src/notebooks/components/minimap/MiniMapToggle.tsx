// Libraries
import React, {FC, useContext} from 'react'

// Contexts
import {AppSettingContext} from 'src/notebooks/context/app'

// Components
import {SlideToggle, InputLabel} from '@influxdata/clockface'

const MiniMapToggle: FC = () => {
  const {miniMapVisibility, onToggleMiniMap} = useContext(AppSettingContext)

  return (
    <>
      <SlideToggle active={miniMapVisibility} onChange={onToggleMiniMap} />
      <InputLabel>Minimap</InputLabel>
    </>
  )
}

export default MiniMapToggle
