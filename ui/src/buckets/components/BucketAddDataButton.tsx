// Libraries
import React, {PureComponent, createRef, RefObject} from 'react'

// Components
import CloudExclude from 'src/shared/components/cloud/CloudExclude'
import {
  Button,
  ButtonRef,
  IconFont,
  ComponentSize,
  ComponentColor,
  Popover,
  PopoverType,
  PopoverPosition,
} from '@influxdata/clockface'

interface Props {
  onAddCollector: () => void
  onAddLineProtocol: () => void
  onAddScraper: () => void
}

export default class BucketAddDataButton extends PureComponent<Props> {
  private triggerRef: RefObject<ButtonRef> = createRef()

  public render() {
    const {onAddCollector, onAddLineProtocol, onAddScraper} = this.props

    return (
      <>
        <Popover
          color={ComponentColor.Secondary}
          type={PopoverType.Outline}
          position={PopoverPosition.ToTheRight}
          triggerRef={this.triggerRef}
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
              <div
                className="bucket-add-data--option"
                onClick={onAddLineProtocol}
              >
                <div
                  className="bucket-add-data--option-header"
                  data-testid="bucket-add-line-protocol"
                >
                  Line Protocol
                </div>
                <div className="bucket-add-data--option-desc">
                  Quickly load an existing line protocol file.
                </div>
              </div>
              <CloudExclude>
                <div className="bucket-add-data--option" onClick={onAddScraper}>
                  <div className="bucket-add-data--option-header">
                    Scrape Metrics
                  </div>
                  <div className="bucket-add-data--option-desc">
                    Add a scrape target to pull data into your bucket.
                  </div>
                </div>
              </CloudExclude>
            </div>
          )}
        />
        <Button
          ref={this.triggerRef}
          text="Add Data"
          testID="add-data--button"
          icon={IconFont.Plus}
          size={ComponentSize.ExtraSmall}
          color={ComponentColor.Secondary}
        />
      </>
    )
  }
}
