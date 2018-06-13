import React, {SFC, ReactElement} from 'react'
import classnames from 'classnames'

interface Props {
  renderLeft: () => ReactElement<any>
  renderRight?: () => ReactElement<any>
  fullWidth: boolean
}

const PageHeader: SFC<Props> = ({renderLeft, renderRight, fullWidth}) => {
  const className = classnames('page-header', {'full-width': fullWidth})

  return (
    <div className={className}>
      <div className="page-header--container">
        <div className="page-header--left">{renderLeft()}</div>
        {renderRight && (
          <div className="page-header--right">{renderRight()}</div>
        )}
      </div>
    </div>
  )
}

export default PageHeader
