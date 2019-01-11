import React, {PureComponent} from 'react'
import _ from 'lodash'

import Container from 'src/clockface/components/overlays/OverlayContainer'
import Heading from 'src/clockface/components/overlays/OverlayHeading'
import Body from 'src/clockface/components/overlays/OverlayBody'
import DragAndDrop from 'src/shared/components/DragAndDrop'

interface Props {
  onDismissOverlay: () => void
  onSave: (script: string, fileName: string) => void
}

class ImportTaskOverlay extends PureComponent<Props> {
  constructor(props: Props) {
    super(props)
  }

  public render() {
    const {onDismissOverlay} = this.props

    return (
      <Container maxWidth={800}>
        <Heading title="Import Task" onDismiss={onDismissOverlay} />
        <Body>
          <DragAndDrop
            submitText="Upload Task"
            fileTypesToAccept={this.validFileExtension}
            handleSubmit={this.handleUploadTask}
          />
        </Body>
      </Container>
    )
  }

  private get validFileExtension(): string {
    return '.flux'
  }

  private handleUploadTask = (
    uploadContent: string,
    fileName: string
  ): void => {
    const {onSave, onDismissOverlay} = this.props
    onSave(uploadContent, fileName)
    onDismissOverlay()
  }
}

export default ImportTaskOverlay
