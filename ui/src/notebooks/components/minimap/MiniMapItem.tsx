// Libraries
import React, {FC} from 'react'
import classnames from 'classnames'

interface Props {
  title: string
  focus: boolean
}

const MiniMapItem: FC<Props> = ({title, focus}) => {
  const className = classnames('notebook-minimap--item', {
    'notebook-minimap--item__focus': focus,
  })

  return <div className={className}>{title}</div>
}

export default MiniMapItem
