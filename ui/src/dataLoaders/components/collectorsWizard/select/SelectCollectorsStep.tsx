// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {ComponentStatus, Form} from 'src/clockface'
import StreamingSelector from 'src/dataLoaders/components/collectorsWizard/select/StreamingSelector'
import OnboardingButtons from 'src/onboarding/components/OnboardingButtons'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'

// Actions
import {
  addPluginBundleWithPlugins,
  removePluginBundleWithPlugins,
  setDataLoadersType,
} from 'src/dataLoaders/actions/dataLoaders'
import {setBucketInfo} from 'src/dataLoaders/actions/steps'

// Types
import {CollectorsStepProps} from 'src/dataLoaders/components/collectorsWizard/CollectorsWizard'
import {
  TelegrafPlugin,
  DataLoaderType,
  BundleName,
} from 'src/types/v2/dataLoaders'
import {Bucket} from 'src/api'
import {AppState} from 'src/types/v2'

export interface OwnProps extends CollectorsStepProps {
  buckets: Bucket[]
}

export interface StateProps {
  type: DataLoaderType
  bucket: string
  telegrafPlugins: TelegrafPlugin[]
  pluginBundles: BundleName[]
}

export interface DispatchProps {
  onAddPluginBundle: typeof addPluginBundleWithPlugins
  onRemovePluginBundle: typeof removePluginBundleWithPlugins
  onSetDataLoadersType: typeof setDataLoadersType
  onSetBucketInfo: typeof setBucketInfo
}

type Props = OwnProps & StateProps & DispatchProps

@ErrorHandling
export class SelectCollectorsStep extends PureComponent<Props> {
  public componentDidMount() {
    if (this.props.type !== DataLoaderType.Streaming) {
      this.props.onSetDataLoadersType(DataLoaderType.Streaming)
    }
  }

  public render() {
    return (
      <div className="onboarding-step">
        <Form onSubmit={this.props.onIncrementCurrentStepIndex}>
          <div className="wizard-step--scroll-area">
            <FancyScrollbar autoHide={false}>
              <div className="wizard-step--scroll-content">
                <h3 className="wizard-step--title">{this.title}</h3>
                <h5 className="wizard-step--sub-title">
                  Telegraf collects and writes metrics to a bucket in InfluxDB.
                </h5>
                <StreamingSelector
                  pluginBundles={this.props.pluginBundles}
                  telegrafPlugins={this.props.telegrafPlugins}
                  onTogglePluginBundle={this.handleTogglePluginBundle}
                  buckets={this.props.buckets}
                  bucket={this.props.bucket}
                  onSelectBucket={this.handleSelectBucket}
                />
              </div>
            </FancyScrollbar>
          </div>
          <OnboardingButtons
            autoFocusNext={true}
            nextButtonStatus={this.nextButtonStatus}
          />
        </Form>
      </div>
    )
  }

  private get nextButtonStatus(): ComponentStatus {
    const {type, telegrafPlugins, buckets} = this.props

    if (!buckets || !buckets.length) {
      return ComponentStatus.Disabled
    }

    const isTypeEmpty = type === DataLoaderType.Empty
    const isStreamingWithoutPlugin =
      type === DataLoaderType.Streaming && !telegrafPlugins.length

    if (isTypeEmpty || isStreamingWithoutPlugin) {
      return ComponentStatus.Disabled
    }

    return ComponentStatus.Default
  }

  private get title(): string {
    const {bucket} = this.props
    return `Select Telegraf Plugins to add to ${bucket || 'your bucket'}`
  }

  private handleSelectBucket = (bucket: Bucket) => {
    const {organization, organizationID, id, name} = bucket

    this.props.onSetBucketInfo(organization, organizationID, name, id)
  }

  private handleTogglePluginBundle = (
    bundle: BundleName,
    isSelected: boolean
  ) => {
    if (isSelected) {
      this.props.onRemovePluginBundle(bundle)

      return
    }

    this.props.onAddPluginBundle(bundle)
  }
}

const mstp = ({
  dataLoading: {
    dataLoaders: {telegrafPlugins, pluginBundles, type},
    steps: {bucket},
  },
}: AppState): StateProps => ({
  type,
  telegrafPlugins,
  bucket,
  pluginBundles,
})

const mdtp: DispatchProps = {
  onSetDataLoadersType: setDataLoadersType,
  onAddPluginBundle: addPluginBundleWithPlugins,
  onRemovePluginBundle: removePluginBundleWithPlugins,
  onSetBucketInfo: setBucketInfo,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(SelectCollectorsStep)
