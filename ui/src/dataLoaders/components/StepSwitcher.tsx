// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import ConfigureDataSourceStep from 'src/dataLoaders/components/configureStep/ConfigureDataSourceStep'
import {ErrorHandling} from 'src/shared/decorators/errors'

// Actions
import {setBucketInfo} from 'src/dataLoaders/actions/steps'

// Types
import {DataLoaderType, DataLoaderStep} from 'src/types/v2/dataLoaders'
import {DataLoaderStepProps} from 'src/dataLoaders/components/DataLoadersWizard'
import {Bucket} from '@influxdata/influx'

interface Props {
  onboardingStepProps: DataLoaderStepProps
  bucketName: string
  buckets: Bucket[]
  type: DataLoaderType
  currentStepIndex: number
  onSetBucketInfo: typeof setBucketInfo
  org: string
}

@ErrorHandling
class StepSwitcher extends PureComponent<Props> {
  public render() {
    const {
      currentStepIndex,
      onboardingStepProps,
      bucketName,
      org,
      buckets,
      type,
    } = this.props

    switch (currentStepIndex) {
      case DataLoaderStep.Configure:
        return (
          <ConfigureDataSourceStep
            {...onboardingStepProps}
            buckets={buckets}
            bucket={bucketName}
            org={org}
            type={type}
          />
        )
      default:
        return <div />
    }
  }
}

export default StepSwitcher
