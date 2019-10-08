// Libraries
import React, {PureComponent} from 'react'

// Components
import CloudExclude from 'src/shared/components/cloud/CloudExclude'
import {
  Button,
  IconFont,
  ComponentSize,
  ComponentColor,
  Popover,
  PopoverType,
  PopoverPosition,
} from '@influxdata/clockface'
import OverlayLink from 'src/overlays/components/OverlayLink'

interface Props {
  onAddCollector: () => void
  bucketID: string
}

export default class BucketAddDataButton extends PureComponent<Props> {
  public render() {
    const {onAddCollector, bucketID} = this.props

    return (
      <Popover
        color={ComponentColor.Secondary}
        type={PopoverType.Outline}
        position={PopoverPosition.ToTheRight}
        distanceFromTrigger={8}
        contents={onHide => (
          <div className="bucket-add-data" onClick={onHide}>
            <div className="bucket-add-data--option" onClick={onAddCollector}>
              <div className="bucket-add-data--option-header">
                Configure Telegraf Agent
              </div>
              <div className="bucket-add-data--option-desc">
                Configure a Telegraf agent to push data into your bucket.
              </div>
            </div>
            <OverlayLink overlayID="write-data-with-line-protocol">
              {onClick => (
                <div className="bucket-add-data--option" onClick={onClick}>
                  <div className="bucket-add-data--option-header">
                    Line Protocol
                  </div>
                  <div className="bucket-add-data--option-desc">
                    Quickly load an existing line protocol file.
                  </div>
                </div>
              )}
            </OverlayLink>
            <CloudExclude>
              <OverlayLink
                overlayID="add-scraper-to-bucket"
                resourceID={bucketID}
              >
                {onClick => (
                  <div className="bucket-add-data--option" onClick={onClick}>
                    <div className="bucket-add-data--option-header">
                      Scrape Metrics
                    </div>
                    <div className="bucket-add-data--option-desc">
                      Add a scrape target to pull data into your bucket.
                    </div>
                  </div>
                )}
              </OverlayLink>
            </CloudExclude>
          </div>
        )}
      >
        <Button
          text="Add Data"
          icon={IconFont.Plus}
          size={ComponentSize.ExtraSmall}
          color={ComponentColor.Secondary}
        />
      </Popover>
    )
  }
}
