// Libraries
import React, {SFC} from 'react'

interface Props {
  children: JSX.Element | JSX.Element[]
}

const PageHeaderLeft: SFC<Props> = ({children}) => (
  <div className="page-header--left">{children}</div>
)

export default PageHeaderLeft
