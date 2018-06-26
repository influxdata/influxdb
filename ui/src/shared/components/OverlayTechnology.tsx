import React, {Component} from 'react'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: JSX.Element
  visible: boolean
}

interface State {
  showChildren: boolean
}

@ErrorHandling
class OverlayTechnology extends Component<Props, State> {
  private animationTimer: number

  constructor(props: Props) {
    super(props)

    this.state = {
      showChildren: false,
    }
  }

  public componentDidUpdate(prevProps) {
    if (prevProps.visible === true && this.props.visible === false) {
      this.animationTimer = window.setTimeout(this.hideChildren, 300)
      return
    }

    if (prevProps.visible === false && this.props.visible === true) {
      this.showChildren()
      return
    }

    return
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
    return `overlay-tech ${visible ? 'show' : ''}`
  }

  private showChildren = (): void => {
    this.setState({showChildren: true})
  }

  private hideChildren = (): void => {
    this.setState({showChildren: false})
    clearTimeout(this.animationTimer)
  }
}

export default OverlayTechnology
