// Libraries
import React, {PureComponent} from 'react'

// Utils
import {downloadTextFile} from 'src/shared/utils/download'

// APIs
import {getTelegrafConfigTOML, createTelegrafConfig} from 'src/onboarding/apis'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {Button} from 'src/clockface'

// Constants
import {getTelegrafConfigFailed} from 'src/shared/copy/v2/notifications'

// Types
import {OnboardingStepProps} from 'src/onboarding/containers/OnboardingWizard'

export interface Props extends OnboardingStepProps {
  dataSource: string
}

@ErrorHandling
class ConfigureDataSourceSwitcher extends PureComponent<Props> {
  public render() {
    const {dataSource} = this.props

    return (
      <div>
        {dataSource}
        <Button text="Click to Download Config" onClick={this.handleDownload} />
      </div>
    )
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
