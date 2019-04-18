// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import {IndexList} from 'src/clockface'
import CollectorRow from 'src/telegrafs/components/CollectorRow'

// Types
import {ITelegraf as Telegraf} from '@influxdata/influx'
import {Sort} from '@influxdata/clockface'
import {SortTypes} from 'src/shared/selectors/sort'
import {AppState} from 'src/types'

//Utils
import {getDeep} from 'src/utils/wrappers'

// Selectors
import {getSortedResource} from 'src/shared/selectors/sort'

type SortKey = keyof Telegraf

interface OwnProps {
  collectors: Telegraf[]
  emptyState: JSX.Element
  onDelete: (telegraf: Telegraf) => void
  onUpdate: (telegraf: Telegraf) => void
  onOpenInstructions: (telegrafID: string) => void
  onFilterChange: (searchTerm: string) => void
  sortKey: string
  sortDirection: Sort
  sortType: SortTypes
  onClickColumn: (nextSort: Sort, sortKey: SortKey) => void
}

interface StateProps {
  sortedCollectors: Telegraf[]
}

type Props = OwnProps & StateProps

class CollectorList extends PureComponent<Props> {
  public state = {
    sortedCollectors: this.props.sortedCollectors,
  }

  componentDidUpdate(prevProps) {
    const {collectors, sortedCollectors, sortKey, sortDirection} = this.props

    if (
      prevProps.sortDirection !== sortDirection ||
      prevProps.sortKey !== sortKey ||
      prevProps.collectors.length !== collectors.length
    ) {
      this.setState({sortedCollectors})
    }
  }

  public render() {
    const {emptyState, sortKey, sortDirection, onClickColumn} = this.props

    return (
      <>
        <IndexList>
          <IndexList.Header>
            <IndexList.HeaderCell
              sortKey={this.headerKeys[0]}
              sort={sortKey === this.headerKeys[0] ? sortDirection : Sort.None}
              columnName="Name"
              width="50%"
              onClick={onClickColumn}
            />
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

  private get headerKeys(): SortKey[] {
    return ['name']
  }

  public get collectorsList(): JSX.Element[] {
    const {
      collectors,
      onDelete,
      onUpdate,
      onOpenInstructions,
      onFilterChange,
    } = this.props
    const {sortedCollectors} = this.state

    if (collectors !== undefined) {
      return sortedCollectors.map(collector => (
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
  }
}

const mstp = (state: AppState, props: OwnProps): StateProps => {
  return {
    sortedCollectors: getSortedResource(state.telegrafs.list, props),
  }
}

export default connect<StateProps, {}, OwnProps>(mstp)(CollectorList)
