// Libraries
import React, {PureComponent} from 'react'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import CardSelectCard from 'src/clockface/components/card_select/CardSelectCard'
import GridSizer from 'src/clockface/components/grid_sizer/GridSizer'

// Types
import {DataLoaderType} from 'src/types/v2/dataLoaders'

export interface Props {
  onSelectTelegrafPlugin: (telegrafPlugin: string) => void
  type: DataLoaderType
}

const DATA_SOURCES_OPTIONS = [
  DataLoaderType.CSV,
  DataLoaderType.Streaming,
  DataLoaderType.LineProtocol,
]

@ErrorHandling
class DataSourceTypeSelector extends PureComponent<Props> {
  public render() {
    return (
      <div className="wizard-step--grid-container-sm">
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
      </div>
    )
  }

  private isCardChecked(dataLoaderType: DataLoaderType) {
    const {type} = this.props

    return dataLoaderType === type
  }

  private handleClick = (telegrafPlugin: string) => () => {
    this.props.onSelectTelegrafPlugin(telegrafPlugin)
  }
}

export default DataSourceTypeSelector
