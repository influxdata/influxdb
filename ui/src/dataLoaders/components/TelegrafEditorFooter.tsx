// Libraries
import React, {PureComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'
import {downloadTextFile} from 'src/shared/utils/download'
import {ComponentColor, Button} from '@influxdata/clockface'
import {isFlagEnabled} from 'src/shared/utils/featureFlag'
import {AppState} from 'src/types'

interface OwnProps {
  onDismiss: () => void
}

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps & OwnProps

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

const mstp = (state: AppState) => {
  const script = state.telegrafEditor.text

  return {
    script,
  }
}

const connector = connect(mstp)

export default connector(TelegrafEditorFooter)
