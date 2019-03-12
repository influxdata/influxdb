import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {get} from 'lodash'
import {client} from 'src/utils/api'

// Components
import {Overlay} from 'src/clockface'
import {Form} from 'src/clockface'
import {Button, ComponentColor} from '@influxdata/clockface'
import {Controlled as ReactCodeMirror} from 'react-codemirror2'

// Actions
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Constants
import {
  resourceSavedAsTemplate,
  saveResourceAsTemplateFailed,
} from 'src/shared/copy/notifications'

// Utils
import {downloadTextFile} from 'src/shared/utils/download'
import {addOrgIDToTemplate} from 'src/shared/utils/resourceToTemplate'

// Styles
import 'src/shared/components/ExportOverlay.scss'

// Types
import {ITemplate} from '@influxdata/influx'

interface OwnProps extends DefaultProps {
  onDismissOverlay: () => void
  resource: ITemplate
  resourceName: string
  orgID: string
}

interface DefaultProps {
  isVisible?: boolean
}

interface DispatchProps {
  notify: typeof notifyAction
}

type Props = OwnProps & DispatchProps

class ExportOverlay extends PureComponent<Props> {
  public static defaultProps: DefaultProps = {
    isVisible: true,
  }

  public render() {
    const {isVisible, resourceName, onDismissOverlay, resource} = this.props
    const options = {
      tabIndex: 1,
      mode: 'json',
      readonly: true,
      lineNumbers: true,
      autoRefresh: true,
      theme: 'time-machine',
      completeSingle: false,
    }
    return (
      <Overlay visible={isVisible}>
        <Overlay.Container maxWidth={800}>
          <Form onSubmit={this.handleExport}>
            <Overlay.Heading
              title={`Export ${resourceName}`}
              onDismiss={onDismissOverlay}
            />
            <Overlay.Body>
              <div className="export-overlay--text-area">
                <ReactCodeMirror
                  autoFocus={false}
                  autoCursor={true}
                  value={JSON.stringify(resource, null, 1)}
                  options={options}
                  onBeforeChange={this.doNothing}
                  onTouchStart={this.doNothing}
                />
              </div>
            </Overlay.Body>
            <Overlay.Footer>
              {this.downloadButton}
              {this.toTemplateButton}
            </Overlay.Footer>
          </Form>
        </Overlay.Container>
      </Overlay>
    )
  }

  private doNothing = () => {}

  private get downloadButton(): JSX.Element {
    return (
      <Button
        text={`Download JSON`}
        onClick={this.handleExport}
        color={ComponentColor.Primary}
      />
    )
  }

  private get toTemplateButton(): JSX.Element {
    return (
      <Button
        text={`Save as template`}
        onClick={this.handleConvertToTemplate}
        color={ComponentColor.Primary}
      />
    )
  }

  private handleExport = (): void => {
    const {resource, resourceName, onDismissOverlay} = this.props
    const name = get(resource, 'name', resourceName)
    downloadTextFile(JSON.stringify(resource, null, 1), `${name}.json`)
    onDismissOverlay()
  }

  private handleConvertToTemplate = async (): Promise<void> => {
    const {resource, onDismissOverlay, orgID, notify, resourceName} = this.props

    const template = addOrgIDToTemplate(resource, orgID)

    try {
      await client.templates.create(template)
      notify(resourceSavedAsTemplate(resourceName))
    } catch (error) {
      notify(saveResourceAsTemplateFailed(resourceName, error))
    }
    onDismissOverlay()
  }
}

const mdtp: DispatchProps = {
  notify: notifyAction,
}

export default connect<{}, DispatchProps, OwnProps>(
  null,
  mdtp
)(ExportOverlay)
