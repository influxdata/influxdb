import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import DragAndDrop from 'src/shared/components/DragAndDrop'
import {
  OverlayTechnology,
  OverlayBody,
  OverlayContainer,
  OverlayHeading,
  Radio,
} from 'src/clockface'

// Constants
import {
  dashboardImportFailed,
  dataWriteFailed,
  dataWritten,
} from 'src/shared/copy/notifications'

// Types
import {Notification} from 'src/types/notifications'
import TextArea from 'src/clockface/components/inputs/TextArea'

enum ImportOption {
  Upload = 'upload',
  Paste = 'paste',
}

interface Props {
  isVisible: boolean
  onDismissOverlay: () => void
  resourceName: string
  isResourceValid: (resource: any) => boolean
  onImport: (resource: any) => void
  notify: (message: Notification) => void
  successNotification?: Notification
  failureNotification?: Notification
}

interface State {
  selectedImportOption: ImportOption
}

export default class ImportDashboardOverlay extends PureComponent<
  Props,
  State
> {
  public static defaultProps: Partial<Props> = {
    successNotification: dataWritten(),
    failureNotification: dataWriteFailed(''),
  }
  public state: State = {
    selectedImportOption: ImportOption.Upload,
  }

  public render() {
    const {onDismissOverlay, isVisible, resourceName} = this.props
    const {selectedImportOption} = this.state

    return (
      <OverlayTechnology visible={isVisible}>
        <OverlayContainer maxWidth={800}>
          <OverlayHeading
            title={`Import ${resourceName}`}
            onDismiss={onDismissOverlay}
          />
          <OverlayBody>
            <div className="save-as--options">
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
          </OverlayBody>
        </OverlayContainer>
      </OverlayTechnology>
    )
  }

  private get importBody(): JSX.Element {
    const {selectedImportOption} = this.state

    if (selectedImportOption === ImportOption.Upload) {
      return (
        <DragAndDrop
          submitText="Upload"
          fileTypesToAccept={this.validFileExtension}
          handleSubmit={this.handleUpload}
        />
      )
    } else if (selectedImportOption === ImportOption.Paste) {
      return <TextArea />
    }
  }

  private handleUpload = (uploadContent: string, fileName: string): void => {
    const {
      notify,
      onImport: onCreate,
      onDismissOverlay,
      successNotification,
      failureNotification,
    } = this.props
    const fileExtensionRegex = new RegExp(`${this.validFileExtension}$`)
    if (!fileName.match(fileExtensionRegex)) {
      notify(dashboardImportFailed(fileName, 'Please import a JSON file'))
      return
    }

    try {
      const resource = JSON.parse(uploadContent)

      if (!_.isEmpty(resource)) {
        onCreate(resource)
        onDismissOverlay()
        notify(successNotification)
      } else {
        notify(failureNotification)
      }
    } catch (error) {
      notify(failureNotification)
    }
  }

  private handleSetImportOption = (selectedImportOption: ImportOption) => {
    this.setState({selectedImportOption})
  }

  private get validFileExtension(): string {
    return '.json'
  }
}
