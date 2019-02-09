// Libraries
import React, {SFC} from 'react'

interface Props {
  children: JSX.Element[]
}

const TabContents: SFC<Props> = ({children}) => (
  <div className="tabs--contents">{children}</div>
)

export default TabContents
