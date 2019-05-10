// Libraries
import React, {PureComponent} from 'react'

// Components
import {Context, Alignment, ComponentSize} from 'src/clockface'

import CloudExclude from 'src/shared/components/cloud/CloudExclude'

import {
  ButtonShape,
  ComponentColor,
  IconFont,
  ComponentSpacer,
  AlignItems,
  FlexDirection,
} from '@influxdata/clockface'

// Types
import {PrettyBucket} from 'src/buckets/components/BucketCard'

interface Props {
  bucket: PrettyBucket
  onDelete: (bucket: PrettyBucket) => void
  onRename: () => void
  onAddCollector: () => void
  onAddLineProtocol: () => void
  onAddScraper: () => void
}

export default class MemberContextMenu extends PureComponent<Props> {
  public render() {
    const {
      bucket,
      onDelete,
      onRename,
      onAddCollector,
      onAddLineProtocol,
      onAddScraper,
    } = this.props

    return (
      <>
        <Context align={Alignment.Center}>
          <ComponentSpacer
            alignItems={AlignItems.Center}
            direction={FlexDirection.Row}
            margin={ComponentSize.Small}
          >
            <Context.Menu
              icon={IconFont.CogThick}
              color={ComponentColor.Danger}
            >
              <Context.Item label="Rename" action={onRename} value={bucket} />
            </Context.Menu>
            <Context.Menu
              icon={IconFont.Trash}
              color={ComponentColor.Danger}
              testID="context-delete-menu"
            >
              <Context.Item
                label="Delete"
                action={onDelete}
                value={bucket}
                testID="context-delete-task"
              />
            </Context.Menu>
            <Context.Menu
              icon={IconFont.Plus}
              text="Add Data"
              shape={ButtonShape.Default}
              color={ComponentColor.Primary}
            >
              <Context.Item
                label="Configure Telegraf Agent"
                description="Configure a Telegraf agent to push data into your bucket."
                action={onAddCollector}
              />
              <Context.Item
                label="Line Protocol"
                description="Quickly load an existing line protocol file."
                action={onAddLineProtocol}
              />
              <CloudExclude>
                <Context.Item
                  label="Scrape Metrics"
                  description="Add a scrape target to pull data into your bucket."
                  action={onAddScraper}
                />
              </CloudExclude>
            </Context.Menu>
          </ComponentSpacer>
        </Context>
      </>
    )
  }
}
