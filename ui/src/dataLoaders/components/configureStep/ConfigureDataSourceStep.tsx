// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import ConfigureDataSourceSwitcher from 'src/dataLoaders/components/configureStep/ConfigureDataSourceSwitcher'

// Types
import {DataLoaderStepProps} from 'src/dataLoaders/components/DataLoadersWizard'
import {DataLoaderType} from 'src/types/v2/dataLoaders'
import {Bucket} from '@influxdata/influx'

export interface OwnProps extends DataLoaderStepProps {
  type: DataLoaderType
  bucket: string
  org: string
  buckets: Bucket[]
}

type Props = OwnProps

@ErrorHandling
export class ConfigureDataSourceStep extends PureComponent<Props> {
  constructor(props: Props) {
    super(props)
  }

  public render() {
    const {type, bucket, org, buckets} = this.props

    return (
      <ConfigureDataSourceSwitcher
        buckets={buckets}
        bucket={bucket}
        org={org}
        dataLoaderType={type}
        onClickNext={this.handleNext}
      />
    )
  }

  private handleNext = async () => {
    const {onIncrementCurrentStepIndex, type, onExit} = this.props

    if (type === DataLoaderType.Scraping) {
      onExit()
      return
    }

    onIncrementCurrentStepIndex()
  }
}

export default ConfigureDataSourceStep
