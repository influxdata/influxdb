// Libraries
import React, {FC} from 'react'
import classnames from 'classnames'

interface Props {
  title: string
  focus: boolean
  visible: boolean
  onClick: () => void
}

const MiniMapItem: FC<Props> = ({title, focus, onClick, visible}) => {
  const className = classnames('notebook-minimap--item', {
    'notebook-minimap--item__focus': focus,
    'notebook-minimap--item__hidden': !visible,
  })

  const handleClick = (): void => {
    onClick()
  }

  return (
    <div className={className} onClick={handleClick}>
      {title}
    </div>
  )
}

export default MiniMapItem
