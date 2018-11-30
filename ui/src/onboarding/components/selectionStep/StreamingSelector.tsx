// Libraries
import React, {PureComponent} from 'react'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import CardSelectCard from 'src/clockface/components/card_select/CardSelectCard'
import GridSizer from 'src/clockface/components/grid_sizer/GridSizer'

// Types
import {DataSource} from 'src/types/v2/dataLoaders'

export interface Props {
  dataSources: DataSource[]
  onToggleDataSource: (dataSource: string, isSelected: boolean) => void
}

const STREAMING_DATA_SOURCES_OPTIONS = [
  'CPU',
  'Docker',
  'InfluxDB',
  'Kubernetes',
  'NGINX',
  'Redis',
]

@ErrorHandling
class StreamingDataSourcesSelector extends PureComponent<Props> {
  public render() {
    return (
      <GridSizer>
        {STREAMING_DATA_SOURCES_OPTIONS.map(ds => {
          return (
            <CardSelectCard
              key={ds}
              id={ds}
              name={ds}
              label={ds}
              checked={this.isCardChecked(ds)}
              onClick={this.handleToggle(ds)}
            />
          )
        })}
      </GridSizer>
    )
  }

  private isCardChecked(dataSource: string) {
    const {dataSources} = this.props

    if (dataSources.find(ds => ds.name === dataSource)) {
      return true
    }
    return false
  }

  private handleToggle = (dataSource: string) => () => {
    this.props.onToggleDataSource(dataSource, this.isCardChecked(dataSource))
  }
}

export default StreamingDataSourcesSelector
