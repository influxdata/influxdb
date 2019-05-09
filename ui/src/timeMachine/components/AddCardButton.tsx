// Libraries
import React, {PureComponent} from 'react'

// Components
import {SquareButton, IconFont} from '@influxdata/clockface'

interface Props {
  collapsible: boolean
  onClick: () => void
}

export default class AddCardButton extends PureComponent<Props> {
  public render() {
    const {onClick} = this.props
    return (
      <SquareButton
        className="query-builder--add-card-button"
        onClick={onClick}
        icon={IconFont.Plus}
      />
    )
  }
}
