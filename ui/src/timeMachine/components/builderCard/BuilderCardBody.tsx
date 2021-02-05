// Libraries
import React, {PureComponent, ReactNode, CSSProperties} from 'react'
import classnames from 'classnames'

// Components
import {DapperScrollbars} from '@influxdata/clockface'

interface Props {
  scrollable: boolean
  addPadding: boolean
  testID: string
  autoHideScrollbars: boolean
  style?: CSSProperties
  className?: string
}

export default class BuilderCardBody extends PureComponent<Props> {
  public static defaultProps = {
    scrollable: true,
    addPadding: true,
    testID: 'builder-card--body',
    autoHideScrollbars: false,
  }

  public render() {
    const {
      scrollable,
      testID,
      autoHideScrollbars,
      style,
      className,
    } = this.props

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

    const classname = classnames('builder-card--body', {
      [`${className}`]: className,
    })

    return (
      <div className={classname} data-testid={testID} style={style}>
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
