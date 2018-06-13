import React, {SFC, ReactElement} from 'react'
import classnames from 'classnames'
import Title from 'src/shared/components/PageHeaderTitle'
import SourceIndicator from 'src/shared/components/SourceIndicator'

interface Props {
  title?: string
  renderTitle?: () => ReactElement<any>
  renderOptions?: () => ReactElement<any>
  fullWidth?: boolean
  sourceIndicator?: boolean
}

const PageHeader: SFC<Props> = ({
  title,
  fullWidth,
  renderTitle,
  renderOptions,
  sourceIndicator,
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
        <div className="page-header--right">
          {sourceIndicator && <SourceIndicator />}
          {renderOptions && renderOptions()}
        </div>
      </div>
    </div>
  )
}

export default PageHeader
