// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'

// Components
import OverlayContainer from 'src/clockface/components/overlays/OverlayContainer'
import OverlayHeading from 'src/clockface/components/overlays/OverlayHeading'
import OverlayBody from 'src/clockface/components/overlays/OverlayBody'
import OverlayFooter from 'src/clockface/components/overlays/OverlayFooter'
import DapperScrollbars from 'src/shared/components/dapperScrollbars/DapperScrollbars'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: JSX.Element
  visible: boolean
  className?: string
}

interface State {
  showChildren: boolean
}

@ErrorHandling
class Overlay extends Component<Props, State> {
  public static Container = OverlayContainer
  public static Heading = OverlayHeading
  public static Body = OverlayBody
  public static Footer = OverlayFooter

  public static getDerivedStateFromProps(props) {
    if (props.visible) {
      return {showChildren: true}
    }

    return {}
  }

  private animationTimer: number

  constructor(props: Props) {
    super(props)

    this.state = {
      showChildren: false,
    }
  }

  public componentDidUpdate(prevProps) {
    if (prevProps.visible && !this.props.visible) {
      clearTimeout(this.animationTimer)
      this.animationTimer = window.setTimeout(this.hideChildren, 300)
    }
  }

  public render() {
    return (
      <DapperScrollbars
        className={this.overlayClass}
        thumbStartColor="#ffffff"
        thumbStopColor="#C9D0FF"
        autoHide={false}
        autoSize={false}
      >
        {this.childContainer}
        <div className="overlay--mask" />
      </DapperScrollbars>
    )
  }

  private get childContainer(): JSX.Element {
    const {children} = this.props
    const {showChildren} = this.state

    if (showChildren) {
      return (
        <div className="overlay--transition" data-testid="overlay-children">
          {children}
        </div>
      )
    }

    return (
      <div className="overlay--transition" data-testid="overlay-children" />
    )
  }

  private get overlayClass(): string {
    const {visible, className} = this.props

    return classnames('overlay', {
      show: visible,
      [`${className}`]: className,
    })
  }

  private hideChildren = (): void => {
    this.setState({showChildren: false})
  }
}

export default Overlay
