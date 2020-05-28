// Libraries
import React, {FC} from 'react'
import classnames from 'classnames'

interface Props {
  title: string
  focus: boolean
  visible: boolean
  index: number
  onClick: (index: number) => void
}

const MiniMapItem: FC<Props> = ({title, focus, onClick, index, visible}) => {
  const className = classnames('notebook-minimap--item', {
    'notebook-minimap--item__focus': focus,
    'notebook-minimap--item__hidden': !visible,
  })

  const handleClick = (): void => {
    onClick(index)
  }

  return (
    <div className={className} onClick={handleClick}>
      {title}
    </div>
  )
}

export default MiniMapItem
