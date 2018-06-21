import React, {Component} from 'react'
import _ from 'lodash'

import Container from 'src/shared/components/overlay/OverlayContainer'
import Heading from 'src/shared/components/overlay/OverlayHeading'
import Body from 'src/shared/components/overlay/OverlayBody'
import SeverityOptions from 'src/logs/components/SeverityOptions'
import ColumnsOptions from 'src/logs/components/ColumnsOptions'
import {SeverityLevel, SeverityColor, SeverityFormat} from 'src/types/logs'
import {DEFAULT_SEVERITY_LEVELS} from 'src/logs/constants'

interface Props {
  severityLevels: SeverityLevel[]
  onUpdateSeverityLevels: (severityLevels: SeverityLevel[]) => void
  onDismissOverlay: () => void
  columns: string[]
  severityFormat: SeverityFormat
  onUpdateSeverityFormat: (format: SeverityFormat) => void
}

interface State {
  workingSeverityLevels: SeverityLevel[]
  workingColumns: string[]
  workingFormat: SeverityFormat
}

class OptionsOverlay extends Component<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      workingSeverityLevels: this.props.severityLevels,
      workingColumns: this.props.columns,
      workingFormat: this.props.severityFormat,
    }
  }

  public render() {
    const {workingSeverityLevels, workingColumns, workingFormat} = this.state

    return (
      <Container maxWidth={700}>
        <Heading title="Configure Log Viewer">
          {this.overlayActionButtons}
        </Heading>
        <Body>
          <div className="row">
            <div className="col-sm-6">
              <SeverityOptions
                severityLevels={workingSeverityLevels}
                onReset={this.handleResetSeverityLevels}
                onChangeSeverityLevel={this.handleChangeSeverityLevel}
                severityFormat={workingFormat}
                onChangeSeverityFormat={this.handleChangeSeverityFormat}
              />
            </div>
            <div className="col-sm-6">
              <ColumnsOptions columns={workingColumns} />
            </div>
          </div>
        </Body>
      </Container>
    )
  }

  private get overlayActionButtons(): JSX.Element {
    const {onDismissOverlay} = this.props

    return (
      <div className="btn-group--right">
        <button className="btn btn-sm btn-default" onClick={onDismissOverlay}>
          Cancel
        </button>
        <button
          className="btn btn-sm btn-success"
          onClick={this.handleSave}
          disabled={this.isSaveDisabled}
        >
          Save
        </button>
      </div>
    )
  }

  private get isSaveDisabled(): boolean {
    const {workingSeverityLevels, workingColumns, workingFormat} = this.state
    const {severityLevels, columns, severityFormat} = this.props

    const severityChanged = !_.isEqual(workingSeverityLevels, severityLevels)
    const columnsChanged = !_.isEqual(workingColumns, columns)
    const formatChanged = !_.isEqual(workingFormat, severityFormat)

    if (severityChanged || columnsChanged || formatChanged) {
      return false
    }

    return true
  }

  private handleSave = () => {
    const {
      onUpdateSeverityLevels,
      onDismissOverlay,
      onUpdateSeverityFormat,
    } = this.props
    const {workingSeverityLevels, workingFormat} = this.state

    onUpdateSeverityFormat(workingFormat)
    onUpdateSeverityLevels(workingSeverityLevels)
    onDismissOverlay()
  }

  private handleResetSeverityLevels = (): void => {
    this.setState({workingSeverityLevels: DEFAULT_SEVERITY_LEVELS})
  }

  private handleChangeSeverityLevel = (severity: string) => (
    override: SeverityColor
  ): void => {
    const workingSeverityLevels = this.state.workingSeverityLevels.map(
      config => {
        if (config.severity === severity) {
          return {...config, override}
        }

        return config
      }
    )

    this.setState({workingSeverityLevels})
  }

  private handleChangeSeverityFormat = (format: SeverityFormat) => () => {
    this.setState({workingFormat: format})
  }
}

export default OptionsOverlay
