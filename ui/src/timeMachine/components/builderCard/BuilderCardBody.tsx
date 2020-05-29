// Libraries
import React, {PureComponent, ReactNode} from 'react'

// Components
import {DapperScrollbars} from '@influxdata/clockface'

interface Props {
  scrollable: boolean
  addPadding: boolean
  testID: string
  autoHideScrollbars: boolean
}

export default class BuilderCardBody extends PureComponent<Props> {
  public static defaultProps = {
    scrollable: true,
    addPadding: true,
    testID: 'builder-card--body',
    autoHideScrollbars: false,
  }

  public render() {
    const {scrollable, testID, autoHideScrollbars} = this.props

    if (scrollable) {
      return (
        <DapperScrollbars
          className="builder-card--body"
          style={{maxWidth: '100%', maxHeight: '100%'}}
          testID={testID}
          autoHide={autoHideScrollbars}
        >
          {this.children}
        </DapperScrollbars>
      )
    }

    return (
      <div className="builder-card--body" data-testid={testID}>
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
