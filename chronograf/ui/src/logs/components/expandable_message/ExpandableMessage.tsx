import React, {Component} from 'react'

import './ExpandableMessage.scss'
import {ClickOutside} from 'src/shared/components/ClickOutside'

interface State {
  expanded: boolean
}

interface Props {
  formattedValue: string | JSX.Element
}

export class ExpandableMessage extends Component<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      expanded: false,
    }
  }

  public render() {
    const formattedValue = `${this.props.formattedValue}`
    const trimmedValue = formattedValue.trimLeft()

    return (
      <ClickOutside onClickOutside={this.handleClickOutside}>
        <div onClick={this.handleClick} className="expandable--message">
          <div className="expandable--text">{trimmedValue}</div>
          <div className={this.isExpanded}>{trimmedValue}</div>
        </div>
      </ClickOutside>
    )
  }

  private get isExpanded() {
    const {expanded} = this.state
    if (expanded) {
      return 'expanded--message'
    } else {
      return 'collapsed--message'
    }
  }

  private handleClick = () => {
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
