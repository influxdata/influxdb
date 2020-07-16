// Libraries
import React, {FC, ReactNode, Children} from 'react'

interface Props {
  title: string
  children: ReactNode
}

const CellFamily: FC<Props> = ({title, children}) => {
  if (Children.count(children) === 0) {
    return null
  }

  return (
    <div className="add-cell-menu--column">
      <h6 className="add-cell-menu--column-title">{title}</h6>
      {children}
    </div>
  )
}

export default CellFamily
