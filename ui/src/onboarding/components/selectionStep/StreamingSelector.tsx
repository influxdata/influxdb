// Libraries
import React, {PureComponent} from 'react'
import uuid from 'uuid'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import CardSelectCard from 'src/clockface/components/card_select/CardSelectCard'
import {GridSizer} from 'src/clockface'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'

// Constants
import {
  PLUGIN_BUNDLE_OPTIONS,
  BUNDLE_LOGOS,
} from 'src/onboarding/constants/pluginConfigs'

// Types
import {TelegrafPlugin, BundleName} from 'src/types/v2/dataLoaders'

export interface Props {
  pluginBundles: BundleName[]
  telegrafPlugins: TelegrafPlugin[]
  onTogglePluginBundle: (telegrafPlugin: string, isSelected: boolean) => void
}

interface State {
  gridSizerUpdateFlag: string
}

const ANIMATION_LENGTH = 400

@ErrorHandling
class StreamingSelector extends PureComponent<Props, State> {
  private scrollMaxHeight = window.innerHeight * 0.6
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
            {PLUGIN_BUNDLE_OPTIONS.map(b => {
              return (
                <CardSelectCard
                  key={b}
                  id={b}
                  name={b}
                  label={b}
                  checked={this.isCardChecked(b)}
                  onClick={this.handleToggle(b)}
                  image={BUNDLE_LOGOS[b]}
                />
              )
            })}
          </GridSizer>
        </div>
      </FancyScrollbar>
    )
  }

  private isCardChecked(bundle: BundleName) {
    const {pluginBundles} = this.props

    if (pluginBundles.find(b => b === bundle)) {
      return true
    }
    return false
  }

  private handleToggle = (bundle: BundleName) => () => {
    this.props.onTogglePluginBundle(bundle, this.isCardChecked(bundle))
  }
}

export default StreamingSelector
