import React, {Component, ReactElement} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {dismissOverlay} from 'src/shared/actions/overlayTechnology'

interface Options {
  dismissOnClickOutside?: boolean
  dismissOnEscape?: boolean
}

interface Props {
  overlayNode?: ReactElement<any>
  options: Options
  handleDismissOverlay: () => void
}

@ErrorHandling
class Overlay extends Component<Props> {
  public render() {
    const {overlayNode, handleDismissOverlay} = this.props

    const overlayClass = `overlay-tech ${overlayNode ? 'show' : ''}`

    return (
      <div className={overlayClass}>
        <div className="overlay--dialog">
          {overlayNode &&
            React.cloneElement(overlayNode, {
              onDismissOverlay: handleDismissOverlay,
            })}
        </div>
        <div className="overlay--mask" onClick={this.handleClickOutside} />
      </div>
    )
  }

  public handleClickOutside = () => {
    const {handleDismissOverlay, options: {dismissOnClickOutside}} = this.props

    if (dismissOnClickOutside) {
      handleDismissOverlay()
    }
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
