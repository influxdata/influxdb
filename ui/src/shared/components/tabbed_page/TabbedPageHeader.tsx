// Libraries
import React, {SFC} from 'react'

interface Props {
  childrenLeft?: JSX.Element[] | JSX.Element
  childrenRight?: JSX.Element[] | JSX.Element
}

const TabbedPageHeader: SFC<Props> = ({childrenLeft, childrenRight}) => {
  let leftHeader = <></>
  let rightHeader = <></>

  if (childrenLeft) {
    leftHeader = <div className="tabbed-page--header-left">{childrenLeft}</div>
  }

  if (childrenRight) {
    rightHeader = (
      <div className="tabbed-page--header-right">{childrenRight}</div>
    )
  }

  return (
    <div className="tabbed-page--header" data-testid="tabbed-page--header">
      {leftHeader}
      {rightHeader}
    </div>
  )
}

export default TabbedPageHeader
