import React, {SFC, ReactElement} from 'react'
import classnames from 'classnames'
import Title from 'src/shared/components/PageHeaderTitle'

interface Props {
  pageTitle?: string
  renderTitle?: () => ReactElement<any>
  renderOptions?: () => ReactElement<any>
  fullWidth: boolean
}

const PageHeader: SFC<Props> = ({
  pageTitle,
  renderTitle,
  renderOptions,
  fullWidth,
}) => {
  if (!pageTitle && !renderTitle) {
    console.error('PageHeader requires either PageTitle or RenderTitle props')
  }

  const className = classnames('page-header', {'full-width': fullWidth})
  let renderLeft = renderTitle

  if (!renderTitle) {
    renderLeft = () => <Title title={pageTitle} />
  }

  return (
    <div className={className}>
      <div className="page-header--container">
        <div className="page-header--left">{renderLeft()}</div>
        {renderOptions && (
          <div className="page-header--Options">{renderOptions()}</div>
        )}
      </div>
    </div>
  )
}

export default PageHeader
