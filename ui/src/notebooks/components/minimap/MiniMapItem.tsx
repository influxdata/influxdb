// Libraries
import React, {FC} from 'react'
import classnames from 'classnames'

interface Props {
  title: string
  focus: boolean
  index: number
  onClick: (index: number) => void
}

const MiniMapItem: FC<Props> = ({title, focus, onClick, index}) => {
  const className = classnames('notebook-minimap--item', {
    'notebook-minimap--item__focus': focus,
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
