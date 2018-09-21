// Libraries
import React, {SFC} from 'react'

interface Props {
  children?: JSX.Element | JSX.Element[]
}

const PageHeaderRight: SFC<Props> = ({children}) => (
  <div className="page-header--right">{children}</div>
)

export default PageHeaderRight
