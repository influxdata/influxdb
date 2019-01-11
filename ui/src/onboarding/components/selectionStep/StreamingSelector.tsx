// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import uuid from 'uuid'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import CardSelectCard from 'src/clockface/components/card_select/CardSelectCard'
import {GridSizer, Input, IconFont, ComponentSize} from 'src/clockface'

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
  searchTerm: string
}

const ANIMATION_LENGTH = 400

@ErrorHandling
class StreamingSelector extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)
    this.state = {
      gridSizerUpdateFlag: uuid.v4(),
      searchTerm: '',
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
    const {gridSizerUpdateFlag, searchTerm} = this.state

    return (
      <div className="wizard-step--grid-container">
        <div className="wizard-step--filter">
          <Input
            size={ComponentSize.Medium}
            icon={IconFont.Search}
            widthPixels={290}
            value={searchTerm}
            onBlur={this.handleFilterBlur}
            onChange={this.handleFilterChange}
            placeholder="Filter Plugins..."
          />
        </div>
        <GridSizer
          wait={ANIMATION_LENGTH}
          recalculateFlag={gridSizerUpdateFlag}
        >
          {this.filteredBundles.map(b => {
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
    )
  }

  private get filteredBundles(): BundleName[] {
    const {searchTerm} = this.state

    return PLUGIN_BUNDLE_OPTIONS.filter(b =>
      b.toLowerCase().includes(searchTerm.toLowerCase())
    )
  }

  private isCardChecked(bundle: BundleName): boolean {
    const {pluginBundles} = this.props

    if (pluginBundles.find(b => b === bundle)) {
      return true
    }
    return false
  }

  private handleToggle = (bundle: BundleName) => (): void => {
    this.props.onTogglePluginBundle(bundle, this.isCardChecked(bundle))
  }

  private handleFilterChange = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({searchTerm: e.target.value})
  }

  private handleFilterBlur = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({searchTerm: e.target.value})
  }
}

export default StreamingSelector
