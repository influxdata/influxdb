// Libraries
import React, {PureComponent} from 'react'

// Components
import {Button, IconFont, ButtonShape} from '@influxdata/clockface'

interface Props {
  collapsible: boolean
  onClick: () => void
}

export default class AddCardButton extends PureComponent<Props> {
  public render() {
    const {onClick} = this.props
    return (
      <Button
        customClass="query-builder--add-card-button"
        onClick={onClick}
        icon={IconFont.Plus}
        shape={ButtonShape.Square}
      />
    )
  }
}
