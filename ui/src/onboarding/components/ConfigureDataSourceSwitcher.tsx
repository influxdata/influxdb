// Libraries
import React, {PureComponent} from 'react'

// Utils
import {downloadTextFile} from 'src/shared/utils/download'

// APIs
import {getTelegrafConfigTOML, createTelegrafConfig} from 'src/onboarding/apis'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {Button} from 'src/clockface'
import DataStreaming from 'src/onboarding/components/DataStreaming'

// Constants
import {getTelegrafConfigFailed} from 'src/shared/copy/v2/notifications'

// Types
import {DataSource} from 'src/types/v2/dataSources'
import {NotificationAction} from 'src/types'

export interface Props {
  dataSources: DataSource[]
  currentIndex: number
  org: string
  username: string
  bucket: string
  notify: NotificationAction
}

@ErrorHandling
class ConfigureDataSourceSwitcher extends PureComponent<Props> {
  public render() {
    const {org, bucket, username} = this.props

    switch (this.configurationStep) {
      case 'Listening':
        return <DataStreaming org={org} username={username} bucket={bucket} />
      case 'CSV':
      case 'Line Protocol':
      default:
        return (
          <div>
            {this.configurationStep}
            <Button
              text="Click to Download Config"
              onClick={this.handleDownload}
            />
          </div>
        )
    }
  }

  private get configurationStep() {
    const {currentIndex, dataSources} = this.props

    if (currentIndex === dataSources.length) {
      return 'Listening'
    }

    return dataSources[currentIndex].name
  }

  private handleDownload = async () => {
    const {notify} = this.props
    try {
      const telegraf = await createTelegrafConfig()
      const config = await getTelegrafConfigTOML(telegraf.id)
      downloadTextFile(config, 'config.toml')
    } catch (error) {
      notify(getTelegrafConfigFailed())
    }
  }
}

export default ConfigureDataSourceSwitcher
