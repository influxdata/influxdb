// Libraries
import React, {Component, MouseEvent} from 'react'
import ReactDOM from 'react-dom'

// Components
import {ClickOutside} from 'src/shared/components/ClickOutside'
import LogsMessage from 'src/logs/components/logs_message/LogsMessage'

// Types
import {NotificationAction} from 'src/types'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface State {
  expanded: boolean
}

interface Props {
  formattedValue: string | JSX.Element
  notify: NotificationAction
  onExpand?: () => void
}

@ErrorHandling
export class ExpandableMessage extends Component<Props, State> {
  private containerRef: React.RefObject<HTMLDivElement>

  constructor(props: Props) {
    super(props)
    this.containerRef = React.createRef()
    this.state = {
      expanded: false,
    }
  }

  public render() {
    return (
      <div
        onClick={this.handleClick}
        className="expandable--message"
        ref={this.containerRef}
      >
        {this.message}
        {this.expandedMessage}
      </div>
    )
  }

  private get message(): JSX.Element {
    const {notify, formattedValue} = this.props
    const valueString = `${formattedValue}`
    const trimmedValue = valueString.trimLeft()

    return <LogsMessage formattedValue={trimmedValue} notify={notify} />
  }

  private get expandedMessage() {
    const {expanded} = this.state

    if (!expanded || !this.containerRef.current) {
      return null
    }

    const portalElement = document.getElementById('expanded-message-container')
    const containerRect = this.containerRef.current.getBoundingClientRect()
    const padding = 8

    const style = {
      top: containerRect.top - padding,
      left: containerRect.left - padding,
      width: containerRect.width + padding + padding,
      padding,
    }

    const message = (
      <ClickOutside onClickOutside={this.handleClickOutside}>
        <div className="expanded--message" style={style}>
          {this.closeExpansionButton}
          {this.message}
        </div>
      </ClickOutside>
    )

    return ReactDOM.createPortal(message, portalElement)
  }

  private get closeExpansionButton(): JSX.Element {
    return (
      <button className="expanded--dismiss" onClick={this.handleClickDismiss} />
    )
  }

  private handleClickDismiss = (e: MouseEvent<HTMLButtonElement>) => {
    e.stopPropagation()

    this.setState({expanded: false})
  }

  private handleClick = () => {
    const {expanded} = this.state
    const {onExpand} = this.props

    if (!expanded && onExpand) {
      onExpand()
    }

    this.setState({
      expanded: true,
    })
  }

  private handleClickOutside = () => {
    this.setState({
      expanded: false,
    })
  }
}

export default ExpandableMessage
