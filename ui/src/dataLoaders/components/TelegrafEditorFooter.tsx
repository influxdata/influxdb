// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {downloadTextFile} from 'src/shared/utils/download'
import {ComponentColor, Button} from '@influxdata/clockface'
import {isFlagEnabled} from 'src/shared/utils/featureFlag'
import {AppState} from 'src/types'

interface OwnProps {
  onDismiss: () => void
}

interface StateProps {
  script: string
}

interface DispatchProps {}

type Props = StateProps & DispatchProps & OwnProps

export class TelegrafEditorFooter extends PureComponent<Props> {
  public render() {
    if (!isFlagEnabled('telegrafEditor')) {
      return false
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
    this.props.onDismiss()
  }
}

const mstp = (state: AppState): StateProps => {
  const script = state.telegrafEditor.text

  return {
    script,
  }
}

const mdtp: DispatchProps = {}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(TelegrafEditorFooter)
