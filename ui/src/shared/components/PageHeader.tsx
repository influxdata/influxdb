import React, {Component, ReactElement} from 'react'
import classnames from 'classnames'
import {connect} from 'react-redux'

import Title from 'src/shared/components/PageHeaderTitle'
import SourceIndicator from 'src/shared/components/SourceIndicator'

interface Props {
  title?: string
  renderTitle?: () => ReactElement<any>
  renderCenter?: () => ReactElement<any>
  renderOptions?: () => ReactElement<any>
  fullWidth?: boolean
  sourceIndicator?: boolean
  inPresentationMode: boolean
}

class PageHeader extends Component<Props> {
  public render() {
    const {inPresentationMode} = this.props

    if (inPresentationMode) {
      return
    }

    return (
      <div className={this.className}>
        <div className="page-header--container">
          <div className="page-header--left">{this.renderLeft}</div>
          {this.renderCenter}
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
    const {title, renderTitle} = this.props

    if (!title && !renderTitle) {
      console.error('PageHeader requires either title or RenderTitle props')
    }

    if (!renderTitle) {
      return <Title title={title} />
    }

    return renderTitle()
  }

  private get renderCenter(): JSX.Element {
    const {renderCenter} = this.props

    if (renderCenter) {
      return <div className="page-header--center">{renderCenter()}</div>
    }
  }

  private get renderRight(): JSX.Element {
    const {renderOptions} = this.props

    if (!renderOptions) {
      return
    }

    return renderOptions()
  }

  private get className(): string {
    const {fullWidth} = this.props

    return classnames('page-header', {'full-width': fullWidth})
  }
}

const mapStateToProps = ({
  app: {
    ephemeral: {inPresentationMode},
  },
}) => ({
  inPresentationMode,
})

export default connect(mapStateToProps)(PageHeader)
