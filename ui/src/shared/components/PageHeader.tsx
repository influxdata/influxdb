import React, {SFC, ReactElement} from 'react'
import classnames from 'classnames'
import Title from 'src/shared/components/PageHeaderTitle'

interface Props {
  title?: string
  renderTitle?: () => ReactElement<any>
  renderOptions?: () => ReactElement<any>
  fullWidth?: boolean
}

const PageHeader: SFC<Props> = ({
  title,
  renderTitle,
  renderOptions,
  fullWidth,
}) => {
  if (!title && !renderTitle) {
    console.error('PageHeader requires either title or RenderTitle props')
  }

  const className = classnames('page-header', {'full-width': fullWidth})
  let renderLeft = renderTitle

  if (!renderTitle) {
    renderLeft = () => <Title title={title} />
  }

  return (
    <div className={className}>
      <div className="page-header--container">
        <div className="page-header--left">{renderLeft()}</div>
        {renderOptions && (
          <div className="page-header--right">{renderOptions()}</div>
        )}
      </div>
    </div>
  )
}

export default PageHeader
