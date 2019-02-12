// Libraries
import React, {SFC} from 'react'

interface Props {
  children: JSX.Element[] | JSX.Element
}

const TabContentsHeader: SFC<Props> = ({children}) => (
  <div className="tabs--contents-header">{children}</div>
)

export default TabContentsHeader
