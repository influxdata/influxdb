// Libraries
import React, {Component} from 'react'

interface Props {
  name: string
  hex: string
  onClick: (hex: string) => void
}

export default class ColorPickerSwatch extends Component<Props> {
  render() {
    const {name, hex} = this.props
    return (
      <div
        className="color-picker--swatch"
        title={name}
        onClick={this.handleClick}
      >
        <span style={{backgroundColor: hex}} />
      </div>
    )
  }

  private handleClick = (): void => {
    this.props.onClick(this.props.hex)
  }
}
