// Libraries
import React, {FC} from 'react'
import cn from 'classnames'

interface Props {
  uploadContent: string
}

const DragInfo: FC<Props> = ({uploadContent}) => {
  const className = cn('drag-and-drop--graphic', {success: uploadContent})

  return <div className={className} />
}

export default DragInfo
