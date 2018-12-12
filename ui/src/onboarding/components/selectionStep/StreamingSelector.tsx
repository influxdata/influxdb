// Libraries
import React, {PureComponent} from 'react'
import uuid from 'uuid'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import CardSelectCard from 'src/clockface/components/card_select/CardSelectCard'
import {GridSizer} from 'src/clockface'

// Constants
import {
  PLUGIN_OPTIONS,
  PLUGIN_LOGOS,
} from 'src/onboarding/constants/pluginConfigs'

// Types
import {TelegrafPlugin} from 'src/types/v2/dataLoaders'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'

export interface Props {
  telegrafPlugins: TelegrafPlugin[]
  onToggleTelegrafPlugin: (telegrafPlugin: string, isSelected: boolean) => void
}
interface State {
  gridSizerUpdateFlag: string
}

const ANIMATION_LENGTH = 400

@ErrorHandling
class StreamingSelector extends PureComponent<Props, State> {
  private scrollMaxHeight = window.innerHeight * 0.45
  constructor(props: Props) {
    super(props)
    this.state = {
      gridSizerUpdateFlag: uuid.v4(),
    }
  }

  public componentDidUpdate(prevProps) {
    const addFirst =
      prevProps.telegrafPlugins.length === 0 &&
      this.props.telegrafPlugins.length > 0

    const removeLast =
      prevProps.telegrafPlugins.length > 0 &&
      this.props.telegrafPlugins.length === 0

    if (addFirst || removeLast) {
      const gridSizerUpdateFlag = uuid.v4()
      this.setState({gridSizerUpdateFlag})
    }
  }

  public render() {
    const {gridSizerUpdateFlag} = this.state

    return (
      <FancyScrollbar
        autoHide={false}
        autoHeight={true}
        maxHeight={this.scrollMaxHeight}
      >
        <div className="wizard-step--grid-container">
          <GridSizer
            wait={ANIMATION_LENGTH}
            recalculateFlag={gridSizerUpdateFlag}
          >
            {PLUGIN_OPTIONS.map(ds => {
              return (
                <CardSelectCard
                  key={ds}
                  id={ds}
                  name={ds}
                  label={ds}
                  checked={this.isCardChecked(ds)}
                  onClick={this.handleToggle(ds)}
                  image={PLUGIN_LOGOS[ds]}
                />
              )
            })}
          </GridSizer>
        </div>
      </FancyScrollbar>
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

export default StreamingSelector
