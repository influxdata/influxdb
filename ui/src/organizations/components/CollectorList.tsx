// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {IndexList} from 'src/clockface'
import CollectorRow from 'src/organizations/components/CollectorRow'

// DummyData
import {Telegraf} from '@influxdata/influx'
import {getDeep} from 'src/utils/wrappers'

interface Props {
  collectors: Telegraf[]
  emptyState: JSX.Element
  onDelete: (telegrafID: string) => void
  onUpdate: (telegraf: Telegraf) => void
  onOpenInstructions: (telegrafID: string) => void
  onOpenTelegrafConfig: (telegrafID: string, telegrafName: string) => void
}

export default class CollectorList extends PureComponent<Props> {
  public render() {
    const {emptyState} = this.props

    return (
      <>
        <IndexList>
          <IndexList.Header>
            <IndexList.HeaderCell columnName="Name" width="50%" />
            <IndexList.HeaderCell columnName="Bucket" width="50%" />
          </IndexList.Header>
          <IndexList.Body columnCount={3} emptyState={emptyState}>
            {this.collectorsList}
          </IndexList.Body>
        </IndexList>
      </>
    )
  }

  public get collectorsList(): JSX.Element[] {
    const {
      collectors,
      onDelete,
      onUpdate,
      onOpenInstructions,
      onOpenTelegrafConfig,
    } = this.props

    if (collectors !== undefined) {
      return collectors.map(collector => (
        <CollectorRow
          key={collector.id}
          collector={collector}
          bucket={getDeep<string>(collector, 'plugins.0.config.bucket', '')}
          onDelete={onDelete}
          onUpdate={onUpdate}
          onOpenInstructions={onOpenInstructions}
          onOpenTelegrafConfig={onOpenTelegrafConfig}
        />
      ))
    }
    return
  }
}
