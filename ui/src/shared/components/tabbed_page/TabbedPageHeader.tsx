// Libraries
import React, {SFC} from 'react'

interface Props {
  children: JSX.Element[] | JSX.Element
}

const TabbedPageHeader: SFC<Props> = ({children}) => (
  <div className="tabbed-page-section--header">{children}</div>
)

export default TabbedPageHeader
