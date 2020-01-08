// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {ComponentColor, Button} from '@influxdata/clockface'

// Actions
import {saveTelegrafConfig} from 'src/dataLoaders/actions/telegrafEditor'

// Utils
import {downloadTextFile} from 'src/shared/utils/download'
import {isFlagEnabled} from 'src/shared/utils/featureFlag'

// Types
import {AppState} from 'src/types'

interface OwnProps {
  onDismiss: () => void
}

interface StateProps {
  script: string
  showSetup: boolean
}

interface DispatchProps {
  onSaveTelegrafConfig: typeof saveTelegrafConfig
}

type Props = StateProps & DispatchProps & OwnProps

export class TelegrafEditorFooter extends PureComponent<Props> {
  public render() {
    if (!isFlagEnabled('telegrafEditor')) {
      return false
    }

    if (this.props.showSetup) {
      return (
        <>
          <Button
            color={ComponentColor.Secondary}
            text="Download Config"
            onClick={this.handleDownloadConfig}
          />
          <Button
            color={ComponentColor.Primary}
            text="Close"
            onClick={this.handleCloseConfig}
          />
        </>
      )
    }

    return (
      <>
        <Button
          color={ComponentColor.Secondary}
          text="Download Config"
          onClick={this.handleDownloadConfig}
        />
        <Button
          color={ComponentColor.Primary}
          text="Save Config"
          onClick={this.handleSaveConfig}
        />
      </>
    )
  }

  private handleDownloadConfig = () => {
    downloadTextFile(this.props.script, 'telegraf', '.conf')
  }

  private handleSaveConfig = () => {
    this.props.onSaveTelegrafConfig()
  }

  private handleCloseConfig = () => {
    this.props.onDismiss()
  }
}

const mstp = (state: AppState): StateProps => {
  const script = state.telegrafEditor.text
  const showSetup = state.telegrafEditor.showSetup

  return {
    script,
    showSetup,
  }
}

const mdtp: DispatchProps = {
  onSaveTelegrafConfig: saveTelegrafConfig,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(TelegrafEditorFooter)
