// Libraries
import React, {FC, useContext} from 'react'

// Components
import {SquareButton, IconFont} from '@influxdata/clockface'
import {NotebookContext, PipeMeta} from 'src/notebooks/context/notebook'

export interface Props {
  index: number
}

const PanelVisibilityToggle: FC<Props> = ({index}) => {
  const {meta, updateMeta} = useContext(NotebookContext)

  const icon = meta[index].visible ? IconFont.EyeOpen : IconFont.EyeClosed

  const handleClick = (): void => {
    updateMeta(index, {
      visible: !meta[index].visible,
    } as PipeMeta)
  }

  return <SquareButton icon={icon} onClick={handleClick} />
}

export default PanelVisibilityToggle
