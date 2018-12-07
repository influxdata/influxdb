// Libraries
import React, {PureComponent} from 'react'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import CardSelectCard from 'src/clockface/components/card_select/CardSelectCard'
import GridSizer from 'src/clockface/components/grid_sizer/GridSizer'

// Types
import {TelegrafPlugin} from 'src/types/v2/dataLoaders'

export interface Props {
  telegrafPlugins: TelegrafPlugin[]
  onToggleTelegrafPlugin: (telegrafPlugin: string, isSelected: boolean) => void
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
      <div className="wizard-step--grid-container-lg">
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
      </div>
    )
  }

  private isCardChecked(telegrafPlugin: string) {
    const {telegrafPlugins} = this.props

    if (telegrafPlugins.find(ds => ds.name === telegrafPlugin)) {
      return true
    }
    return false
  }

  private handleToggle = (telegrafPlugin: string) => () => {
    this.props.onToggleTelegrafPlugin(
      telegrafPlugin,
      this.isCardChecked(telegrafPlugin)
    )
  }
}

export default StreamingDataSourcesSelector
