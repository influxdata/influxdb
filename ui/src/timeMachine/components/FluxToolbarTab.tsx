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
    onClick(id)
  }

  return (
    <div
      className={toolbarTabClass}
      onClick={handleClick}
      title={name}
      data-testid={testID}
    >
      {name}
    </div>
  )
}

export default ToolbarTab
