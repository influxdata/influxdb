// Libraries
import React, {Component} from 'react'
import _ from 'lodash'
import classnames from 'classnames'
import {Scrollbars} from '@influxdata/react-custom-scrollbars'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  className: string
  maxHeight: number
  autoHide: boolean
  autoHeight: boolean
  style: React.CSSProperties
  hideTracksWhenNotNeeded: boolean
  setScrollTop: (value: React.MouseEvent<HTMLElement>) => void
  scrollTop?: number
  scrollLeft?: number
  thumbStartColor?: string
  thumbStopColor?: string
}

@ErrorHandling
class FancyScrollbar extends Component<Props> {
  public static defaultProps = {
    className: '',
    autoHide: true,
    autoHeight: false,
    hideTracksWhenNotNeeded: true,
    maxHeight: null,
    style: {},
    setScrollTop: () => {},
  }

  private ref: React.RefObject<Scrollbars>

  constructor(props) {
    super(props)
    this.ref = React.createRef<Scrollbars>()
  }

  public updateScroll() {
    const ref = this.ref.current
    if (ref && !_.isNil(this.props.scrollTop)) {
      ref.scrollTop(this.props.scrollTop)
    }

    if (ref && !_.isNil(this.props.scrollLeft)) {
      ref.scrollLeft(this.props.scrollLeft)
    }
  }

  public componentDidMount() {
    this.updateScroll()
  }

  public componentDidUpdate() {
    this.updateScroll()
  }

  public render() {
    const {
      autoHide,
      autoHeight,
      children,
      className,
      maxHeight,
      setScrollTop,
      style,
      thumbStartColor,
      thumbStopColor,
      hideTracksWhenNotNeeded,
    } = this.props

    return (
      <Scrollbars
        className={classnames('fancy-scroll--container', {
          [className]: className,
        })}
        ref={this.ref}
        style={style}
        onScroll={setScrollTop}
        autoHide={autoHide}
        autoHideTimeout={1000}
        autoHideDuration={250}
        autoHeight={autoHeight}
        autoHeightMax={maxHeight}
        thumbStartColor={thumbStartColor}
        thumbStopColor={thumbStopColor}
        hideTracksWhenNotNeeded={hideTracksWhenNotNeeded}
      >
        {children}
      </Scrollbars>
    )
  }
}

export default FancyScrollbar
