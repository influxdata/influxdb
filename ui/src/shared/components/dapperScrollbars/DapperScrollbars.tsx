// Libraries
import React, {Component, CSSProperties, ReactNode} from 'react'
import _ from 'lodash'
import classnames from 'classnames'
import Scrollbar from 'react-scrollbars-custom'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface PassedProps {
  children: JSX.Element | JSX.Element[] | ReactNode
}

interface DefaultProps {
  className?: string
  removeTracksWhenNotUsed?: boolean
  removeTrackYWhenNotUsed?: boolean
  removeTrackXWhenNotUsed?: boolean
  noScrollX?: boolean
  noScrollY?: boolean
  noScroll?: boolean
  thumbStartColor?: string
  thumbStopColor?: string
  style?: CSSProperties
  autoHide?: boolean
  autoSize?: boolean
}

type Props = PassedProps & DefaultProps

@ErrorHandling
class DapperScrollbars extends Component<Props> {
  public static defaultProps: DefaultProps = {
    removeTracksWhenNotUsed: true,
    removeTrackYWhenNotUsed: true,
    removeTrackXWhenNotUsed: true,
    noScrollX: false,
    noScrollY: false,
    noScroll: false,
    thumbStartColor: '#00C9FF',
    thumbStopColor: '#9394FF',
    autoHide: false,
    autoSize: true,
  }

  public render() {
    const {
      removeTracksWhenNotUsed,
      removeTrackYWhenNotUsed,
      removeTrackXWhenNotUsed,
      noScrollX,
      noScrollY,
      className,
      autoHide,
      autoSize,
      noScroll,
      children,
      style,
    } = this.props

    const classname = classnames('dapper-scrollbars', {
      'dapper-scrollbars--autohide': autoHide,
      [`${className}`]: className,
    })

    return (
      <Scrollbar
        translateContentSizesToHolder={autoSize}
        className={classname}
        style={style}
        noDefaultStyles={false}
        removeTracksWhenNotUsed={removeTracksWhenNotUsed}
        removeTrackYWhenNotUsed={removeTrackYWhenNotUsed}
        removeTrackXWhenNotUsed={removeTrackXWhenNotUsed}
        noScrollX={noScrollX}
        noScrollY={noScrollY}
        noScroll={noScroll}
        wrapperProps={{className: 'dapper-scrollbars--wrapper'}}
        trackXProps={{className: 'dapper-scrollbars--track-x'}}
        thumbXProps={{
          style: this.thumbXStyle,
          className: 'dapper-scrollbars--thumb-x',
        }}
        trackYProps={{className: 'dapper-scrollbars--track-y'}}
        thumbYProps={{
          style: this.thumbYStyle,
          className: 'dapper-scrollbars--thumb-y',
        }}
      >
        {children}
      </Scrollbar>
    )
  }

  private get thumbXStyle(): CSSProperties {
    const {thumbStartColor, thumbStopColor} = this.props

    return {
      background: `linear-gradient(to right,  ${thumbStartColor} 0%,${thumbStopColor} 100%)`,
    }
  }

  private get thumbYStyle(): CSSProperties {
    const {thumbStartColor, thumbStopColor} = this.props

    return {
      background: `linear-gradient(to bottom,  ${thumbStartColor} 0%,${thumbStopColor} 100%)`,
    }
  }
}

export default DapperScrollbars
