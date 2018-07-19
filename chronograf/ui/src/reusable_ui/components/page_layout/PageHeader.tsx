import React, {Component, ReactElement} from 'react'
import classnames from 'classnames'

import Title from 'src/reusable_ui/components/page_layout/PageHeaderTitle'
import SourceIndicator from 'src/shared/components/SourceIndicator'

interface Props {
  titleText?: string
  titleComponents?: ReactElement<any>
  optionsComponents?: ReactElement<any>
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
    const {titleText, titleComponents} = this.props

    if (!titleText && !titleComponents) {
      throw new Error(
        'PageHeader requires either titleText or titleComponents prop'
      )
    }

    if (!titleComponents) {
      return <Title title={titleText} />
    }

    return titleComponents
  }

  private get renderRight(): JSX.Element {
    const {optionsComponents} = this.props

    if (!optionsComponents) {
      return
    }

    return optionsComponents
  }

  private get className(): string {
    const {fullWidth} = this.props

    return classnames('page-header', {'full-width': fullWidth})
  }
}

export default PageHeader
