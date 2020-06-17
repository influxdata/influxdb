// Libraries
import React, {FC, useContext} from 'react'

// Components
import {SquareButton, IconFont} from '@influxdata/clockface'
import {NotebookContext, PipeMeta} from 'src/notebooks/context/notebook'

// Utils
import {event} from 'src/notebooks/shared/event'

export interface Props {
  index: number
}

const PanelVisibilityToggle: FC<Props> = ({index}) => {
  const {meta, updateMeta} = useContext(NotebookContext)

  const icon = meta[index].visible ? IconFont.EyeOpen : IconFont.EyeClosed
  const title = meta[index].visible ? 'Collapse cell' : 'Expand cell'

  const handleClick = (): void => {
    event('Panel Visibility Toggled', {
      state: !meta[index].visible ? 'true' : 'false',
    })

    updateMeta(index, {
      visible: !meta[index].visible,
    } as PipeMeta)
  }

  return <SquareButton icon={icon} onClick={handleClick} titleText={title} />
}

export default PanelVisibilityToggle
