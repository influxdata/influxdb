// Libraries
import React, {PureComponent} from 'react'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import CardSelectCard from 'src/clockface/components/card_select/CardSelectCard'
import GridSizer from 'src/clockface/components/grid_sizer/GridSizer'

// Types
import {DataSource} from 'src/types/v2/dataSources'
import {StreamingOptions} from 'src/onboarding/components/SelectDataSourceStep'

export interface Props {
  dataSources: DataSource[]
  onSelectDataSource: (dataSource: string) => void
  streaming: StreamingOptions
}

const DATA_SOURCES_OPTIONS = ['CSV', 'Streaming', 'Line Protocol']

@ErrorHandling
class DataSourceSelector extends PureComponent<Props> {
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
              onClick={this.handleToggle(ds)}
            />
          )
        })}
      </GridSizer>
    )
  }

  private isCardChecked(dataSource: string) {
    const {dataSources, streaming} = this.props
    if (dataSource === 'Streaming') {
      return streaming === StreamingOptions.Selected
    }

    if (dataSources.find(ds => ds.name === dataSource)) {
      return true
    }
    return false
  }

  private handleToggle = (dataSource: string) => () => {
    this.props.onSelectDataSource(dataSource)
  }
}

export default DataSourceSelector
