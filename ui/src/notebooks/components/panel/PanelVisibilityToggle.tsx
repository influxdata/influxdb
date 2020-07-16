// Libraries
import React, {FC, useContext} from 'react'

// Components
import {SquareButton, IconFont} from '@influxdata/clockface'
import {NotebookContext} from 'src/notebooks/context/notebook.current'

// Utils
import {event} from 'src/cloud/utils/reporting'

import {DataID, PipeData} from 'src/notebooks'

export interface Props {
  id: DataID<PipeData>
}

const PanelVisibilityToggle: FC<Props> = ({id}) => {
  const {notebook} = useContext(NotebookContext)
  const meta = notebook.meta.get(id)

  const icon = meta.visible ? IconFont.EyeOpen : IconFont.EyeClosed
  const title = meta.visible ? 'Collapse cell' : 'Expand cell'

  const handleClick = (): void => {
    event('Panel Visibility Toggled', {
      state: !meta.visible ? 'true' : 'false',
    })

    notebook.meta.update(id, {
      visible: !meta.visible,
    })
  }

  return <SquareButton icon={icon} onClick={handleClick} titleText={title} />
}

export default PanelVisibilityToggle
