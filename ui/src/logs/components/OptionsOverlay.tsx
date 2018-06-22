import React, {Component} from 'react'
import _ from 'lodash'

import Container from 'src/shared/components/overlay/OverlayContainer'
import Heading from 'src/shared/components/overlay/OverlayHeading'
import Body from 'src/shared/components/overlay/OverlayBody'
import SeverityOptions from 'src/logs/components/SeverityOptions'
import ColumnsOptions from 'src/logs/components/ColumnsOptions'
import {
  SeverityLevel,
  SeverityColor,
  SeverityFormat,
  LogsTableColumn,
} from 'src/types/logs'
import {DEFAULT_SEVERITY_LEVELS} from 'src/logs/constants'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  severityLevels: SeverityLevel[]
  onUpdateSeverityLevels: (severityLevels: SeverityLevel[]) => void
  onDismissOverlay: () => void
  columns: LogsTableColumn[]
  onUpdateColumns: (columns: LogsTableColumn[]) => void
  severityFormat: SeverityFormat
  onUpdateSeverityFormat: (format: SeverityFormat) => void
}

interface State {
  workingSeverityLevels: SeverityLevel[]
  workingColumns: LogsTableColumn[]
  workingFormat: SeverityFormat
}

@ErrorHandling
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
      <Container maxWidth={800}>
        <Heading title="Configure Log Viewer">
          {this.overlayActionButtons}
        </Heading>
        <Body>
          <div className="row">
            <div className="col-sm-5">
              <SeverityOptions
                severityLevels={workingSeverityLevels}
                onReset={this.handleResetSeverityLevels}
                onChangeSeverityLevel={this.handleChangeSeverityLevel}
                severityFormat={workingFormat}
                onChangeSeverityFormat={this.handleChangeSeverityFormat}
              />
            </div>
            <div className="col-sm-7">
              <ColumnsOptions
                columns={workingColumns}
                onMoveColumn={this.handleMoveColumn}
                onUpdateColumn={this.handleUpdateColumn}
              />
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
      onUpdateColumns,
    } = this.props
    const {workingSeverityLevels, workingFormat, workingColumns} = this.state

    onUpdateSeverityFormat(workingFormat)
    onUpdateSeverityLevels(workingSeverityLevels)
    onUpdateColumns(workingColumns)
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

  private handleMoveColumn = (dragIndex, hoverIndex) => {
    const {workingColumns} = this.state

    const draggedField = workingColumns[dragIndex]

    const columnsRemoved = _.concat(
      _.slice(workingColumns, 0, dragIndex),
      _.slice(workingColumns, dragIndex + 1)
    )

    const columnsAdded = _.concat(
      _.slice(columnsRemoved, 0, hoverIndex),
      [draggedField],
      _.slice(columnsRemoved, hoverIndex)
    )

    this.setState({workingColumns: columnsAdded})
  }

  private handleUpdateColumn = (column: LogsTableColumn) => {
    const workingColumns = this.state.workingColumns.map(wc => {
      if (wc.internalName === column.internalName) {
        return column
      }

      return wc
    })

    this.setState({workingColumns})
  }
}

export default OptionsOverlay
