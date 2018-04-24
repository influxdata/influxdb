import React, {PureComponent, ReactElement} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {dismissOverlay} from 'src/shared/actions/overlayTechnology'

interface Options {
  dismissOnClickOutside?: boolean
  dismissOnEscape?: boolean
  transitionTime?: number
}

interface Props {
  overlayNode?: ReactElement<any>
  options: Options
  handleDismissOverlay: () => void
}

interface State {
  visible: boolean
}

@ErrorHandling
class Overlay extends PureComponent<Props, State> {
  private animationTimer: number

  constructor(props) {
    super(props)

    this.state = {
      visible: false,
    }
  }

  public componentDidUpdate(prevProps) {
    if (prevProps.overlayNode === null && this.props.overlayNode) {
      return this.setState({visible: true})
    }
  }

  public render() {
    const {overlayNode} = this.props

    return (
      <div className={this.overlayClass}>
        <div className="overlay--dialog">
          {overlayNode &&
            React.cloneElement(overlayNode, {
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

  private get options(): Options {
    const {options} = this.props
    const defaultOptions = {
      dismissOnClickOutside: false,
      dismissOnEscape: false,
      transitionTime: 300,
    }

    return {...defaultOptions, ...options}
  }

  public handleClickOutside = () => {
    const {handleDismissOverlay, options: {dismissOnClickOutside}} = this.props

    if (dismissOnClickOutside) {
      handleDismissOverlay()
    }
  }

  public handleAnimateDismiss = () => {
    const {transitionTime} = this.options
    this.setState({visible: false})
    this.animationTimer = window.setTimeout(this.handleDismiss, transitionTime)
  }

  public handleDismiss = () => {
    const {handleDismissOverlay} = this.props
    handleDismissOverlay()
    clearTimeout(this.animationTimer)
  }
}

const mapStateToProps = ({overlayTechnology: {overlayNode, options}}) => ({
  overlayNode,
  options,
})

const mapDispatchToProps = dispatch => ({
  handleDismissOverlay: bindActionCreators(dismissOverlay, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(Overlay)
