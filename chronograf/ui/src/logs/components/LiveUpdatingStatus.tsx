import React, {PureComponent} from 'react'

import {Button, ButtonShape, ComponentColor, IconFont} from 'src/clockface'

interface Props {
  liveUpdating: boolean
  onChangeLiveUpdatingStatus: () => void
}

export default class LiveUpdatingStatus extends PureComponent<Props> {
  public render() {
    const {onChangeLiveUpdatingStatus} = this.props

    return (
      <Button
        customClass="logs-viewer--mode-toggle"
        shape={ButtonShape.Square}
        color={this.color}
        icon={this.icon}
        onClick={onChangeLiveUpdatingStatus}
      />
    )
  }

  private get icon(): IconFont {
    const {liveUpdating} = this.props

    if (liveUpdating) {
      return IconFont.Play
    }

    return IconFont.Pause
  }

  private get color(): ComponentColor {
    const {liveUpdating} = this.props

    if (liveUpdating) {
      return ComponentColor.Primary
    }

    return ComponentColor.Default
  }
}
