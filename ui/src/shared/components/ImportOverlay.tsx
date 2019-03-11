import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import DragAndDrop from 'src/shared/components/DragAndDrop'
import {Overlay, Radio, ComponentStatus} from 'src/clockface'
import {Button, ComponentColor} from '@influxdata/clockface'

// Styles
import 'src/shared/components/ImportOverlay.scss'

// Types
import TextArea from 'src/clockface/components/inputs/TextArea'

enum ImportOption {
  Upload = 'upload',
  Paste = 'paste',
}

interface Props {
  onDismissOverlay: () => void
  resourceName: string
  onSubmit: (importString: string) => void
  isVisible?: boolean
}

interface State {
  selectedImportOption: ImportOption
  importContent: string
}

export default class ImportOverlay extends PureComponent<Props, State> {
  public static defaultProps: Partial<Props> = {
    isVisible: true,
  }

  public state: State = {
    selectedImportOption: ImportOption.Upload,
    importContent: '',
  }

  public render() {
    const {isVisible, resourceName} = this.props
    const {selectedImportOption} = this.state

    return (
      <Overlay visible={isVisible}>
        <Overlay.Container maxWidth={800}>
          <Overlay.Heading
            title={`Import ${resourceName}`}
            onDismiss={this.onDismiss}
          />
          <Overlay.Body>
            <div className="import--options">
              <Radio>
                <Radio.Button
                  active={selectedImportOption === ImportOption.Upload}
                  value={ImportOption.Upload}
                  onClick={this.handleSetImportOption}
                >
                  Upload File
                </Radio.Button>
                <Radio.Button
                  active={selectedImportOption === ImportOption.Paste}
                  value={ImportOption.Paste}
                  onClick={this.handleSetImportOption}
                >
                  Paste JSON
                </Radio.Button>
              </Radio>
            </div>
            {this.importBody}
          </Overlay.Body>
          <Overlay.Footer>{this.submitButton}</Overlay.Footer>
        </Overlay.Container>
      </Overlay>
    )
  }

  private get importBody(): JSX.Element {
    const {selectedImportOption, importContent} = this.state

    if (selectedImportOption === ImportOption.Upload) {
      return (
        <DragAndDrop
          submitText="Upload"
          handleSubmit={this.handleSetImportContent}
          submitOnDrop={true}
          submitOnUpload={true}
          onCancel={this.clearImportContent}
        />
      )
    }
    if (selectedImportOption === ImportOption.Paste) {
      return (
        <TextArea
          value={importContent}
          onChange={this.handleSetImportContent}
        />
      )
    }
  }

  private get submitButton(): JSX.Element {
    const {resourceName} = this.props
    const {selectedImportOption, importContent} = this.state
    const isEnabled =
      selectedImportOption === ImportOption.Paste ||
      (selectedImportOption === ImportOption.Upload && importContent)
    const status = isEnabled
      ? ComponentStatus.Default
      : ComponentStatus.Disabled

    return (
      <Button
        text={`Import JSON as ${resourceName}`}
        onClick={this.submit}
        color={ComponentColor.Primary}
        status={status}
      />
    )
  }

  private submit = () => {
    const {importContent} = this.state
    const {onSubmit} = this.props

    onSubmit(importContent)
    this.clearImportContent()
  }
  private clearImportContent = () => {
    this.handleSetImportContent('')
  }

  private onDismiss = () => {
    const {onDismissOverlay} = this.props
    this.clearImportContent()
    onDismissOverlay()
  }

  private handleSetImportOption = (selectedImportOption: ImportOption) => {
    this.clearImportContent()
    this.setState({selectedImportOption})
  }

  private handleSetImportContent = (importContent: string): void => {
    this.setState({importContent})
  }
}
