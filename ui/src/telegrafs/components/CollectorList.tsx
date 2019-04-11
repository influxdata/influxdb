// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {IndexList} from 'src/clockface'
import CollectorRow from 'src/telegrafs/components/CollectorRow'

// DummyData
import {ITelegraf as Telegraf} from '@influxdata/influx'
import {getDeep} from 'src/utils/wrappers'

interface Props {
  collectors: Telegraf[]
  emptyState: JSX.Element
  onDelete: (telegraf: Telegraf) => void
  onUpdate: (telegraf: Telegraf) => void
  onOpenInstructions: (telegrafID: string) => void
  onFilterChange: (searchTerm: string) => void
}

export default class CollectorList extends PureComponent<Props> {
  public render() {
    const {emptyState} = this.props

    return (
      <>
        <IndexList>
          <IndexList.Header>
            <IndexList.HeaderCell columnName="Name" width="50%" />
            <IndexList.HeaderCell columnName="Bucket" width="25%" />
            <IndexList.HeaderCell columnName="" width="25%" />
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
      onFilterChange,
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
          onFilterChange={onFilterChange}
        />
      ))
    }
    return
  }
}
