// Libraries
import React, {PureComponent} from 'react'

// Components
import {
  ComponentSize,
  IndexList,
  ConfirmationButton,
  Alignment,
} from 'src/clockface'
import {ResourceOwner} from 'src/api'

interface Props {
  scraper: ResourceOwner
}

export default class BucketRow extends PureComponent<Props> {
  public render() {
    return (
      <>
        <IndexList.Row>
          <IndexList.Cell>name</IndexList.Cell>
          <IndexList.Cell>bucket</IndexList.Cell>
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
