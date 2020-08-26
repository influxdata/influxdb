// Libraries
import React, {PureComponent} from 'react'

// Components
import {
  Button,
  SpinnerContainer,
  TechnoSpinner,
  Overlay,
  ComponentSize,
} from '@influxdata/clockface'
import {Controlled as ReactCodeMirror} from 'react-codemirror2'
import CopyButton from 'src/shared/components/CopyButton'

// Types
import {ComponentColor} from '@influxdata/clockface'
import {RemoteDataState, DashboardTemplate} from 'src/types'
import {DocumentCreate} from '@influxdata/influx'

interface Props {
  onDismissOverlay: () => void
  resource: DashboardTemplate | DocumentCreate
  overlayHeading: string
  status: RemoteDataState
  isVisible: boolean
}

export default class ViewOverlay extends PureComponent<Props> {
  public static defaultProps = {
    isVisible: true,
  }

  public render() {
    const {isVisible, overlayHeading, onDismissOverlay, status} = this.props

    return (
      <Overlay visible={isVisible}>
        <Overlay.Container maxWidth={800}>
          <Overlay.Header title={overlayHeading} onDismiss={onDismissOverlay} />
          <Overlay.Body>
            <SpinnerContainer
              loading={status}
              spinnerComponent={<TechnoSpinner />}
            >
              {this.overlayBody}
            </SpinnerContainer>
          </Overlay.Body>
          <Overlay.Footer>
            {this.closeButton}
            {this.copyButton}
          </Overlay.Footer>
        </Overlay.Container>
      </Overlay>
    )
  }

  private doNothing = () => {}

  private get overlayBody(): JSX.Element {
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
      <div className="export-overlay--text-area">
        <ReactCodeMirror
          autoFocus={false}
          autoCursor={true}
          value={this.resourceText}
          options={options}
          onBeforeChange={this.doNothing}
          onTouchStart={this.doNothing}
        />
      </div>
    )
  }

  private get resourceText(): string {
    return JSON.stringify(this.props.resource, null, 1)
  }

  private get copyButton(): JSX.Element {
    return (
      <CopyButton
        textToCopy={this.resourceText}
        contentName={this.props.overlayHeading}
        size={ComponentSize.Small}
        color={ComponentColor.Secondary}
      />
    )
  }

  private get closeButton(): JSX.Element {
    return (
      <Button
        text="Close"
        onClick={this.props.onDismissOverlay}
        color={ComponentColor.Primary}
      />
    )
  }
}
