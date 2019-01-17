// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import uuid from 'uuid'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import CardSelectCard from 'src/clockface/components/card_select/CardSelectCard'
import {
  GridSizer,
  Input,
  IconFont,
  ComponentSize,
  Dropdown,
  FormElement,
  Grid,
  Columns,
} from 'src/clockface'

// Constants
import {
  PLUGIN_BUNDLE_OPTIONS,
  BUNDLE_LOGOS,
} from 'src/onboarding/constants/pluginConfigs'

// Types
import {TelegrafPlugin, BundleName} from 'src/types/v2/dataLoaders'
import {Bucket} from 'src/api'

export interface Props {
  buckets: Bucket[]
  bucket: string
  selectedBucket: string
  pluginBundles: BundleName[]
  telegrafPlugins: TelegrafPlugin[]
  onTogglePluginBundle: (telegrafPlugin: string, isSelected: boolean) => void
  onSelectBucket: (bucket: Bucket) => void
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
        <Grid.Row>
          <Grid.Column widthSM={Columns.Five}>
            <FormElement label="Bucket">
              <Dropdown
                selectedID={this.selectedBucketID}
                onChange={this.handleSelectBucket}
              >
                {this.dropdownBuckets}
              </Dropdown>
            </FormElement>
          </Grid.Column>
          <Grid.Column widthSM={Columns.Five} offsetSM={Columns.Two}>
            <FormElement label="">
              <Input
                customClass={'wizard-step--filter'}
                size={ComponentSize.Small}
                icon={IconFont.Search}
                value={searchTerm}
                onBlur={this.handleFilterBlur}
                onChange={this.handleFilterChange}
                placeholder="Filter Plugins..."
              />
            </FormElement>
          </Grid.Column>
        </Grid.Row>
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

  private handleSelectBucket = (bucketName: string) => {
    const bucket = this.props.buckets.find(b => b.name === bucketName)

    this.props.onSelectBucket(bucket)
  }

  private get selectedBucketID(): string {
    const {bucket, selectedBucket, buckets} = this.props

    return selectedBucket || bucket || _.get(buckets, '0.name', '')
  }

  private get dropdownBuckets(): JSX.Element[] {
    const {buckets} = this.props

    return buckets.map(b => (
      <Dropdown.Item key={b.name} value={b.name} id={b.name}>
        {b.name}
      </Dropdown.Item>
    ))
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
