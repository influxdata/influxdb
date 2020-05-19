// Libraries
import React, {FC} from 'react'

// Components
import {SquareButton, IconFont} from '@influxdata/clockface'

// Types
import {NotebookPanelVisibility} from 'src/notebooks/components/panel/NotebookPanel'

interface Props {
  visibility: NotebookPanelVisibility
  onToggle: (visibility: NotebookPanelVisibility) => void
}

const PanelVisibilityToggle: FC<Props> = ({visibility, onToggle}) => {
  const icon = visibility === 'visible' ? IconFont.EyeOpen : IconFont.EyeClosed

  const handleClick = (): void => {
    if (visibility === 'hidden') {
      onToggle('visible')
    } else if (visibility === 'visible') {
      onToggle('hidden')
    }
  }

  return <SquareButton icon={icon} onClick={handleClick} />
}

export default PanelVisibilityToggle
