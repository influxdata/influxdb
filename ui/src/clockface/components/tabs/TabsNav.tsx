// Libraries
import React, {SFC} from 'react'

interface Props {
  children: JSX.Element | JSX.Element[]
}

const TabsNav: SFC<Props> = ({children}) => (
  <div className="tabs--nav">
    <div className="tabs--nav-tabs">{children}</div>
  </div>
)

export default TabsNav
