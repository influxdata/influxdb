// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {Form, Radio, Button} from '@influxdata/clockface'
import {Overlay} from 'src/clockface'
import DragAndDrop from 'src/shared/components/DragAndDrop'
import TextArea from 'src/clockface/components/inputs/TextArea'

// Types
import {
  ButtonType,
  ComponentColor,
  ComponentStatus,
} from '@influxdata/clockface'

enum ImportOption {
  Upload = 'upload',
  Paste = 'paste',
}

interface OwnProps {
  onDismissOverlay: () => void
  resourceName: string
  onSubmit: (importString: string, orgID: string) => void
  isVisible?: boolean
}

interface State {
  selectedImportOption: ImportOption
  importContent: string
}

type Props = OwnProps & WithRouterProps

class ImportOverlay extends PureComponent<Props, State> {
  public static defaultProps: {isVisible: boolean} = {
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
          <Form onSubmit={this.submit}>
            <Overlay.Heading
              title={`Import ${resourceName}`}
              onDismiss={this.onDismiss}
            />
            <Overlay.Body>
              <div className="import--options">
                <Radio>
                  <Radio.Button
                    id={ImportOption.Upload}
                    active={selectedImportOption === ImportOption.Upload}
                    value={ImportOption.Upload}
                    onClick={this.handleSetImportOption}
                    titleText="Upload"
                  >
                    Upload File
                  </Radio.Button>
                  <Radio.Button
                    id={ImportOption.Paste}
                    active={selectedImportOption === ImportOption.Paste}
                    value={ImportOption.Paste}
                    onClick={this.handleSetImportOption}
                    titleText="Paste"
                  >
                    Paste JSON
                  </Radio.Button>
                </Radio>
              </div>
              {this.importBody}
            </Overlay.Body>
            <Overlay.Footer>{this.submitButton}</Overlay.Footer>
          </Form>
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
        color={ComponentColor.Primary}
        status={status}
        type={ButtonType.Submit}
      />
    )
  }

  private submit = () => {
    const {importContent} = this.state
    const {
      onSubmit,
      params: {orgID},
    } = this.props

    onSubmit(importContent, orgID)
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

export default withRouter<OwnProps>(ImportOverlay)
