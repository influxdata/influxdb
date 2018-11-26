// Libraries
import React, {PureComponent} from 'react'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import CardSelectCard from 'src/clockface/components/card_select/CardSelectCard'
import GridSizer from 'src/clockface/components/grid_sizer/GridSizer'

// Types
import {DataSourceType} from 'src/types/v2/dataSources'

export interface Props {
  onSelectDataSource: (dataSource: string) => void
  type: DataSourceType
}

const DATA_SOURCES_OPTIONS = [
  DataSourceType.CSV,
  DataSourceType.Streaming,
  DataSourceType.LineProtocol,
]

@ErrorHandling
class DataSourceTypeSelector extends PureComponent<Props> {
  public render() {
    return (
      <GridSizer>
        {DATA_SOURCES_OPTIONS.map(ds => {
          return (
            <CardSelectCard
              key={ds}
              id={ds}
              name={ds}
              label={ds}
              checked={this.isCardChecked(ds)}
              onClick={this.handleClick(ds)}
            />
          )
        })}
      </GridSizer>
    )
  }

  private isCardChecked(dataSource: DataSourceType) {
    const {type} = this.props

    return dataSource === type
  }

  private handleClick = (dataSource: string) => () => {
    this.props.onSelectDataSource(dataSource)
  }
}

export default DataSourceTypeSelector
