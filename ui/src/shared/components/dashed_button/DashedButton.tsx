// Libraries
import React, {FC, MouseEvent} from 'react'

interface Props {
  text: string
  onClick: (e: MouseEvent) => void
}

const DashedButton: FC<Props> = ({text, onClick}) => {
  return (
    <button className="dashed-button" onClick={onClick}>
      {text}
    </button>
  )
}

export default DashedButton
