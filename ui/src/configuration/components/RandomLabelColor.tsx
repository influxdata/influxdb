import React, {Component} from 'react'
import _ from 'lodash'

// Utils
import {randomPresetColor} from 'src/configuration/utils/labels'
import {IconFont} from 'src/clockface'

// Styles
import 'src/configuration/components/RandomLabelColor.scss'

interface Props {
  colorHex: string
  onClick: (newRandomHex: string) => void
}

export default class RandomLabelColorButton extends Component<Props> {
  public render() {
    const {colorHex} = this.props
    return (
      <button
        className="button button-sm button-default random-color--button "
        onClick={this.handleClick}
      >
        <div
          className="label-colors--swatch"
          style={{
            backgroundColor: colorHex,
          }}
        />
        <span className={`button-icon icon ${IconFont.Refresh}`} />
      </button>
    )
  }

  private handleClick = () => {
    this.props.onClick(randomPresetColor())
  }
}
