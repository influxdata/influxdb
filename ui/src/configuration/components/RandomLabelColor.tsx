import React, {Component} from 'react'
import _ from 'lodash'

// Utils
import {randomPresetColor} from 'src/configuration/utils/labels'
import {IconFont} from 'src/clockface'
import {validateHexCode} from 'src/configuration/utils/labels'

// Constants
import {INPUT_ERROR_COLOR} from 'src/configuration/constants/LabelColors'

interface Props {
  colorHex: string
  onClick: (newRandomHex: string) => void
}

export default class RandomLabelColorButton extends Component<Props> {
  public render() {
    return (
      <button
        className="button button-sm button-default random-color--button "
        onClick={this.handleClick}
        title="Randomize label color"
      >
        <div
          className="label-colors--swatch"
          style={{
            backgroundColor: this.colorHex,
          }}
        />
        <span className={`button-icon icon ${IconFont.Refresh}`} />
      </button>
    )
  }

  private get colorHex(): string {
    const {colorHex} = this.props

    if (validateHexCode(colorHex)) {
      return INPUT_ERROR_COLOR
    }

    return colorHex
  }

  private handleClick = () => {
    this.props.onClick(randomPresetColor())
  }
}
