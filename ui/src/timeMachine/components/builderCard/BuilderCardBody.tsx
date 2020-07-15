// Libraries
import React, {PureComponent, ReactNode} from 'react'

// Components
import {DapperScrollbars} from '@influxdata/clockface'

interface Props {
  scrollable: boolean
  addPadding: boolean
  testID: string
  autoHideScrollbars: boolean
  style?: CSSProperties
}

export default class BuilderCardBody extends PureComponent<Props> {
  public static defaultProps = {
    scrollable: true,
    addPadding: true,
    testID: 'builder-card--body',
    autoHideScrollbars: false,
  }

  public render() {
    const {scrollable, testID, autoHideScrollbars, style} = this.props

    if (scrollable) {
      const scrollbarStyles = {maxWidth: '100%', maxHeight: '100%', ...style}

      return (
        <DapperScrollbars
          className="builder-card--body"
          style={scrollbarStyles}
          testID={testID}
          autoHide={autoHideScrollbars}
        >
          {this.children}
        </DapperScrollbars>
      )
    }

    return (
      <div className="builder-card--body" data-testid={testID} style={style}>
        {this.children}
      </div>
    )
  }

  private get children(): JSX.Element | ReactNode {
    const {addPadding, children} = this.props

    if (addPadding) {
      return <div className="builder-card--contents">{children}</div>
    }

    return children
  }
}
