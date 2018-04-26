import React, {PureComponent, ReactElement} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {dismissOverlay} from 'src/shared/actions/overlayTechnology'

interface Props {
  OverlayNode?: ReactElement<any>
  dismissOnClickOutside?: boolean
  dismissOnEscape?: boolean
  transitionTime?: number
  handleDismissOverlay: () => void
}

interface State {
  visible: boolean
}

@ErrorHandling
class Overlay extends PureComponent<Props, State> {
  public static defaultProps: Partial<Props> = {
    dismissOnClickOutside: false,
    dismissOnEscape: false,
    transitionTime: 300,
  }

  private animationTimer: number

  constructor(props) {
    super(props)

    this.state = {
      visible: false,
    }
  }

  public componentDidUpdate(prevProps) {
    if (prevProps.OverlayNode === null && this.props.OverlayNode) {
      return this.setState({visible: true})
    }
  }

  public render() {
    const {OverlayNode} = this.props

    return (
      <div className={this.overlayClass}>
        <div className="overlay--dialog">
          {OverlayNode &&
            React.cloneElement(OverlayNode, {
              onDismissOverlay: this.handleAnimateDismiss,
            })}
        </div>
        <div className="overlay--mask" onClick={this.handleClickOutside} />
      </div>
    )
  }

  private get overlayClass(): string {
    const {visible} = this.state
    return `overlay-tech ${visible ? 'show' : ''}`
  }

  public handleClickOutside = () => {
    const {handleDismissOverlay, dismissOnClickOutside} = this.props

    if (dismissOnClickOutside) {
      handleDismissOverlay()
    }
  }

  public handleAnimateDismiss = () => {
    const {transitionTime} = this.props
    this.setState({visible: false})
    this.animationTimer = window.setTimeout(this.handleDismiss, transitionTime)
  }

  public handleDismiss = () => {
    const {handleDismissOverlay} = this.props
    handleDismissOverlay()
    clearTimeout(this.animationTimer)
  }
}

const mapStateToProps = ({
  overlayTechnology: {
    OverlayNode,
    options: {dismissOnClickOutside, dismissOnEscape, transitionTime},
  },
}) => ({
  OverlayNode,
  dismissOnClickOutside,
  dismissOnEscape,
  transitionTime,
})

const mapDispatchToProps = dispatch => ({
  handleDismissOverlay: bindActionCreators(dismissOverlay, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(Overlay)
