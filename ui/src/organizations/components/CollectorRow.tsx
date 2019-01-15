// Libraries
import React, {PureComponent} from 'react'

// Components
import {
  ComponentSize,
  IndexList,
  ConfirmationButton,
  Alignment,
  Button,
  ComponentColor,
} from 'src/clockface'
import {Telegraf} from 'src/api'

interface Props {
  collector: Telegraf
  bucket: string
}

export default class BucketRow extends PureComponent<Props> {
  public render() {
    const {collector, bucket} = this.props
    return (
      <>
        <IndexList.Row>
          <IndexList.Cell>{collector.name}</IndexList.Cell>
          <IndexList.Cell>{bucket}</IndexList.Cell>
          <IndexList.Cell>
            <Button
              size={ComponentSize.Small}
              color={ComponentColor.Secondary}
              text={'Download Config'}
            />
          </IndexList.Cell>
          <IndexList.Cell revealOnHover={true} alignment={Alignment.Right}>
            <ConfirmationButton
              size={ComponentSize.ExtraSmall}
              text="Delete"
              confirmText="Confirm"
            />
          </IndexList.Cell>
        </IndexList.Row>
      </>
    )
  }
}
