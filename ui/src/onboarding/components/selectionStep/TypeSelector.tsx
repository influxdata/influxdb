// Libraries
import React, {PureComponent} from 'react'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import CardSelectCard from 'src/clockface/components/card_select/CardSelectCard'
import {GridSizer} from 'src/clockface'
import {IconCSV, IconLineProtocol, IconStreaming} from 'src/onboarding/graphics'

// Types
import {DataLoaderType} from 'src/types/v2/dataLoaders'

export interface Props {
  onSelectDataLoaderType: (type: string) => void
  type: DataLoaderType
}

const DATA_SOURCES_OPTIONS = [
  DataLoaderType.CSV,
  DataLoaderType.Streaming,
  DataLoaderType.LineProtocol,
]

const DATA_SOURCES_LOGOS = {
  [DataLoaderType.CSV]: IconCSV,
  [DataLoaderType.Streaming]: IconStreaming,
  [DataLoaderType.LineProtocol]: IconLineProtocol,
}

@ErrorHandling
class TypeSelector extends PureComponent<Props> {
  public render() {
    return (
      <div className="wizard-step--grid-container">
        <GridSizer cellWidth={200}>
          {DATA_SOURCES_OPTIONS.map(ds => {
            return (
              <CardSelectCard
                key={ds}
                id={ds}
                name={ds}
                label={ds}
                checked={this.isCardChecked(ds)}
                onClick={this.handleClick(ds)}
                image={DATA_SOURCES_LOGOS[ds]}
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

  private handleClick = (type: string) => () => {
    this.props.onSelectDataLoaderType(type)
  }
}

export default TypeSelector
