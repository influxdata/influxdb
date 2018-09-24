// Libraries
import React, {Component} from 'react'
import _ from 'lodash'
import classnames from 'classnames'
import {Scrollbars} from 'react-custom-scrollbars'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface DefaultProps {
  autoHide: boolean
  autoHeight: boolean
  maxHeight: number
  setScrollTop: (value: React.MouseEvent<JSX.Element>) => void
  style: React.CSSProperties
}

interface Props {
  className?: string
  scrollTop?: number
  scrollLeft?: number
}

@ErrorHandling
class FancyScrollbar extends Component<Props & Partial<DefaultProps>> {
  public static defaultProps = {
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

  public handleMakeDiv = (className: string) => (props): JSX.Element => {
    return <div {...props} className={`fancy-scroll--${className}`} />
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
        renderTrackHorizontal={this.handleMakeDiv('track-h')}
        renderTrackVertical={this.handleMakeDiv('track-v')}
        renderThumbHorizontal={this.handleMakeDiv('thumb-h')}
        renderThumbVertical={this.handleMakeDiv('thumb-v')}
        renderView={this.handleMakeDiv('view')}
      >
        {children}
      </Scrollbars>
    )
  }
}

export default FancyScrollbar
