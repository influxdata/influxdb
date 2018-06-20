import React, {Component, ReactElement} from 'react'
import classnames from 'classnames'

import Title from 'src/shared/components/PageHeaderTitle'
import SourceIndicator from 'src/shared/components/SourceIndicator'

interface Props {
  titleText?: string
  renderTitle?: ReactElement<any>
  renderPageControls?: ReactElement<any>
  fullWidth?: boolean
  sourceIndicator?: boolean
  inPresentationMode?: boolean
}

class PageHeader extends Component<Props> {
  public render() {
    const {inPresentationMode} = this.props

    if (inPresentationMode) {
      return null
    }

    return (
      <div className={this.className}>
        <div className="page-header--container">
          <div className="page-header--left">{this.renderLeft}</div>
          <div className="page-header--right">
            {this.sourceIndicator}
            {this.renderRight}
          </div>
        </div>
      </div>
    )
  }

  private get sourceIndicator(): JSX.Element {
    const {sourceIndicator} = this.props

    if (!sourceIndicator) {
      return
    }

    return <SourceIndicator />
  }

  private get renderLeft(): JSX.Element {
    const {titleText, renderTitle} = this.props

    if (!titleText && !renderTitle) {
      console.error('PageHeader requires either titleText or RenderTitle props')
    }

    if (!renderTitle) {
      return <Title title={titleText} />
    }

    return renderTitle
  }

  private get renderRight(): JSX.Element {
    const {renderPageControls} = this.props

    if (!renderPageControls) {
      return
    }

    return renderPageControls
  }

  private get className(): string {
    const {fullWidth} = this.props

    return classnames('page-header', {'full-width': fullWidth})
  }
}

export default PageHeader
