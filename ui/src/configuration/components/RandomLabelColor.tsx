import React, {Component} from 'react'
import _ from 'lodash'

import {Button} from '@influxdata/clockface'

import {PRESET_LABEL_COLORS} from 'src/configuration/constants/LabelColors'

// Styles
import 'src/configuration/components/RandomLabelColor.scss'

interface Props {
  colorHex: string
  onClick: (newRandomHex: string) => void
}

export default class RandomLabelColorButton extends Component<Props> {
  public componentDidMount() {
    this.handleClick()
  }

  public render() {
    const {colorHex} = this.props
    return (
      <button
        className="button button-sm button-default random-color--button "
        onClick={this.handleClick}
      >
        New Color
        <div
          className="label-colors--swatch"
          style={{
            backgroundColor: colorHex,
          }}
        />
      </button>
    )
  }

  private handleClick = () => {
    this.props.onClick(this.randomColor())
  }

  private randomColor(): string {
    return _.sample(PRESET_LABEL_COLORS.slice(1)).colorHex
  }
}
