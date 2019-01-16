// Libraries
import React, {PureComponent} from 'react'

// Components
import {
  ComponentSize,
  IndexList,
  ConfirmationButton,
  Alignment,
} from 'src/clockface'
import {ScraperTargetResponse} from 'src/api'

interface Props {
  scraper: ScraperTargetResponse
}

export default class BucketRow extends PureComponent<Props> {
  public render() {
    const {scraper} = this.props
    return (
      <>
        <IndexList.Row>
          <IndexList.Cell>{scraper.name}</IndexList.Cell>
          <IndexList.Cell>{scraper.bucket}</IndexList.Cell>
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
