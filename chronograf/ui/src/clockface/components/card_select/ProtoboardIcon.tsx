// Libraries
import React, {PureComponent} from 'react'

interface Props {
  displayText?: string
}

class ProtoboardIcon extends PureComponent<Props> {
  public static defaultProps: Partial<Props> = {
    displayText: '',
  }

  public render() {
    return (
      <div className="protoboard-icon">
        <svg
          version="1.1"
          id="protoboard_icon"
          x="0px"
          y="0px"
          viewBox="0 0 80 65"
        >
          <rect
            className="protoboard-icon--shape"
            x="2"
            y="6"
            width="32"
            height="28"
            rx="2"
            ry="2"
          />
          <rect
            className="protoboard-icon--shape"
            x="40"
            y="6"
            width="38"
            height="9"
            rx="2"
            ry="2"
          />
          <rect
            className="protoboard-icon--shape"
            x="2"
            y="41"
            width="32"
            height="21"
            rx="2"
            ry="2"
          />
          <rect
            className="protoboard-icon--text-shape"
            x="40"
            y="22"
            width="38"
            height="40"
            rx="2"
            ry="2"
          />
          <text
            transform="matrix(1 0 0 1 59 57)"
            textAnchor="middle"
            className="protoboard-icon--text"
          >
            {this.displayInitial}
          </text>
        </svg>
      </div>
    )
  }

  private get displayInitial() {
    const {displayText} = this.props
    return displayText.substring(0, 1).toUpperCase()
  }
}

export default ProtoboardIcon
