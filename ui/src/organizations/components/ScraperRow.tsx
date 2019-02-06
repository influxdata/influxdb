// Libraries
import React, {PureComponent} from 'react'

// Components
import {
  ComponentSize,
  IndexList,
  ConfirmationButton,
  Alignment,
} from 'src/clockface'
import {ScraperTargetResponse} from '@influxdata/influx'
import EditableName from 'src/shared/components/EditableName'

interface Props {
  scraper: ScraperTargetResponse
  onDeleteScraper: (scraper) => void
  onUpdateScraper: (scraper: ScraperTargetResponse) => void
}

export default class ScraperRow extends PureComponent<Props> {
  public render() {
    const {scraper, onDeleteScraper} = this.props
    return (
      <>
        <IndexList.Row>
          <IndexList.Cell>
            <EditableName
              onUpdate={this.handleUpdateScraper}
              name={scraper.url}
            />
          </IndexList.Cell>
          <IndexList.Cell>{scraper.bucket}</IndexList.Cell>
          <IndexList.Cell revealOnHover={true} alignment={Alignment.Right}>
            <ConfirmationButton
              size={ComponentSize.ExtraSmall}
              text="Delete"
              confirmText="Confirm"
              returnValue={scraper}
              onConfirm={onDeleteScraper}
            />
          </IndexList.Cell>
        </IndexList.Row>
      </>
    )
  }

  private handleUpdateScraper = (name: string) => {
    const {onUpdateScraper, scraper} = this.props
    onUpdateScraper({...scraper, url: name})
  }
}
