// Libraries
import React, {FC} from 'react'
import classnames from 'classnames'

interface Props {
  value: any
  onClick: (value: any) => void
  selected: boolean
  text: string
}

const SelectorListItem: FC<Props> = ({value, onClick, selected, text}) => {
  const className = classnames('data-source--list-item', {
    'data-source--list-item__selected': selected,
  })

  const handleClick = (): void => {
    onClick(value)
  }

  return (
    <div className={className} onClick={handleClick}>
      {text}
    </div>
  )
}

export default SelectorListItem
