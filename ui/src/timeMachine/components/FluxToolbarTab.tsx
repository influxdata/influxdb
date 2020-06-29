// Libraries
import React, {FC} from 'react'
import classnames from 'classnames'

interface Props {
  onClick: (id: string) => void
  id: string
  name: string
  active: boolean
  testID?: string
}

const ToolbarTab: FC<Props> = ({
  onClick,
  name,
  active,
  testID = 'toolbar-tab',
  id,
}) => {
  const toolbarTabClass = classnames('flux-toolbar--tab', {
    'flux-toolbar--tab__active': active,
  })

  const handleClick = (): void => {
    if (active) {
      onClick('none')
    } else {
      onClick(id)
    }
  }

  return (
    <div
      className={toolbarTabClass}
      onClick={handleClick}
      title={name}
      data-testid={testID}
    >
      <div className="flux-toolbar--tab-label">{name}</div>
    </div>
  )
}

export default ToolbarTab
