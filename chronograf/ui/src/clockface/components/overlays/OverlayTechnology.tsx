import React, {Component} from 'react'
import classnames from 'classnames'
import {ErrorHandling} from 'src/shared/decorators/errors'
import './overlays.scss'

interface Props {
  children: JSX.Element
  visible: boolean
}

interface State {
  showChildren: boolean
}

@ErrorHandling
class OverlayTechnology extends Component<Props, State> {
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
      <div className={this.overlayClass}>
        {this.childContainer}
        <div className="overlay--mask" />
      </div>
    )
  }

  private get childContainer(): JSX.Element {
    const {children} = this.props
    const {showChildren} = this.state

    if (showChildren) {
      return (
        <div className="overlay--dialog" data-test="overlay-children">
          {children}
        </div>
      )
    }

    return <div className="overlay--dialog" data-test="overlay-children" />
  }

  private get overlayClass(): string {
    const {visible} = this.props

    return classnames('overlay-tech', {show: visible})
  }

  private hideChildren = (): void => {
    this.setState({showChildren: false})
  }
}

export default OverlayTechnology
