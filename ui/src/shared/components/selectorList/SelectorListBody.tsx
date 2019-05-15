// Libraries
import React, {PureComponent, ReactNode} from 'react'

// Components
import {DapperScrollbars} from '@influxdata/clockface'

interface Props {
  scrollable: boolean
  addPadding: boolean
  testID: string
}

export default class SelectorListBody extends PureComponent<Props> {
  public static defaultProps = {
    scrollable: true,
    addPadding: true,
    testID: 'selector-list--body',
  }

  public render() {
    const {scrollable, testID} = this.props

    if (scrollable) {
      return (
        <DapperScrollbars
          className="selector-list--body"
          style={{maxWidth: '100%', maxHeight: '100%'}}
          testID={testID}
        >
          {this.children}
        </DapperScrollbars>
      )
    }

    return (
      <div className="selector-list--body" data-testid={testID}>
        {this.children}
      </div>
    )
  }

  private get children(): JSX.Element | ReactNode {
    const {addPadding, children} = this.props

    if (addPadding) {
      return <div className="selector-list--contents">{children}</div>
    }

    return children
  }
}
