// Libraries
import React, {Component} from 'react'
import _ from 'lodash'
import classnames from 'classnames'
import {Scrollbars} from '@influxdata/react-custom-scrollbars'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface DefaultProps {
  autoHide: boolean
  autoHeight: boolean
  maxHeight: number
  setScrollTop: (value: React.MouseEvent<HTMLElement>) => void
  style: React.CSSProperties
}

interface Props {
  className?: string
  scrollTop?: number
  scrollLeft?: number
  thumbStartColor?: string
  thumbStopColor?: string
}

@ErrorHandling
class FancyScrollbar extends Component<Props & Partial<DefaultProps>> {
  public static defaultProps: DefaultProps = {
    autoHide: true,
    autoHeight: false,
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
      >
        {children}
      </Scrollbars>
    )
  }
}

export default FancyScrollbar
