import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {get} from 'lodash'
import {client} from 'src/utils/api'

// Components
import {
  OverlayTechnology,
  OverlayBody,
  OverlayContainer,
  OverlayHeading,
  OverlayFooter,
} from 'src/clockface'
import {Button, ComponentColor} from '@influxdata/clockface'
import {Controlled as ReactCodeMirror} from 'react-codemirror2'

// Actions
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Constants
import {
  dashboardSavedAsTemplate,
  saveDashboardAsTemplateFailed,
} from 'src/shared/copy/notifications'

// Utils
import {downloadTextFile} from 'src/shared/utils/download'
import {addOrgIDToTemplate} from 'src/shared/utils/resourceToTemplate'

// Styles
import 'src/shared/components/ExportOverlay.scss'

// Types
import {ITemplate} from '@influxdata/influx'

interface OwnProps {
  onDismissOverlay: () => void
  resource: ITemplate
  orgID: string
  resourceName: string
  isVisible?: boolean
}

interface DispatchProps {
  notify: typeof notifyAction
}

type Props = OwnProps & DispatchProps

class ExportOverlay extends PureComponent<Props> {
  public static defaultProps: Partial<Props> = {
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
      <OverlayTechnology visible={isVisible}>
        <OverlayContainer maxWidth={800}>
          <OverlayHeading
            title={`Export ${resourceName}`}
            onDismiss={onDismissOverlay}
          />
          <OverlayBody>
            <div className="export-overlay--text-area">
              <ReactCodeMirror
                autoFocus={true}
                autoCursor={true}
                value={JSON.stringify(resource, null, 1)}
                options={options}
                onBeforeChange={this.doNothing}
                onTouchStart={this.doNothing}
              />
            </div>
          </OverlayBody>
          <OverlayFooter>
            {this.downloadButton}
            {this.toTemplateButton}
          </OverlayFooter>
        </OverlayContainer>
      </OverlayTechnology>
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
    const {resourceName} = this.props
    if (resourceName == 'Dashboard') {
      return (
        <Button
          text={`Save as template`}
          onClick={this.handleConvertToTemplate}
          color={ComponentColor.Primary}
        />
      )
    }
  }

  private handleExport = (): void => {
    const {resource, resourceName, onDismissOverlay} = this.props
    const name = get(resource, 'name', resourceName)
    downloadTextFile(JSON.stringify(resource, null, 1), `${name}.json`)
    onDismissOverlay()
  }

  private handleConvertToTemplate = async (): Promise<void> => {
    const {resource, onDismissOverlay, orgID, notify} = this.props

    const template = addOrgIDToTemplate(resource, orgID)

    try {
      await client.templates.create(template)
      notify(dashboardSavedAsTemplate())
    } catch (error) {
      notify(saveDashboardAsTemplateFailed(error))
    }
    onDismissOverlay()
  }
}

const mdtp: DispatchProps = {
  notify: notifyAction,
}

export default connect<null, DispatchProps, OwnProps>(
  null,
  mdtp
)(ExportOverlay)
