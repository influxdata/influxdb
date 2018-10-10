import React, {Component} from 'react'
import _ from 'lodash'

import {Button, ComponentStatus} from 'src/clockface'
import Container from 'src/clockface/components/overlays/OverlayContainer'
import Heading from 'src/clockface/components/overlays/OverlayHeading'
import Body from 'src/clockface/components/overlays/OverlayBody'
import SeverityOptions from 'src/logs/components/options_overlay/SeverityOptions'
import ColumnsOptions from 'src/logs/components/options_overlay/ColumnsOptions'

import {
  SeverityLevelColor,
  SeverityColor,
  SeverityFormat,
  LogsTableColumn,
  SeverityLevelOptions,
  LogConfig,
} from 'src/types/logs'

import {DEFAULT_SEVERITY_LEVELS} from 'src/logs/constants'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  severityLevelColors: SeverityLevelColor[]
  columns: LogsTableColumn[]
  severityFormat: SeverityFormat
  onDismissOverlay: () => void
  onSave: (config: Partial<LogConfig>) => Promise<void>
}

interface State {
  workingLevelColumns: SeverityLevelColor[]
  workingColumns: LogsTableColumn[]
  workingFormat: SeverityFormat
}

@ErrorHandling
class OptionsOverlay extends Component<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      workingLevelColumns: this.props.severityLevelColors,
      workingColumns: this.props.columns,
      workingFormat: this.props.severityFormat,
    }
  }

  public shouldComponentUpdate(__, nextState: State) {
    const isColorsDifferent = !_.isEqual(
      nextState.workingLevelColumns,
      this.state.workingLevelColumns
    )
    const isFormatDifferent = !_.isEqual(
      nextState.workingFormat,
      this.state.workingFormat
    )
    const isColumnsDifferent = !_.isEqual(
      nextState.workingColumns,
      this.state.workingColumns
    )

    if (isColorsDifferent || isFormatDifferent || isColumnsDifferent) {
      return true
    }
    return false
  }

  public render() {
    const {workingLevelColumns, workingColumns, workingFormat} = this.state

    return (
      <Container maxWidth={800}>
        <Heading title="Configure Log Viewer">
          {this.overlayActionButtons}
        </Heading>
        <Body>
          <div className="row">
            <div className="col-sm-5">
              <SeverityOptions
                severityLevelColors={workingLevelColumns}
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
        <Button text="Cancel" onClick={onDismissOverlay} />
        <Button
          onClick={this.handleSave}
          status={this.isSaveDisabled}
          text="Save"
        />
      </div>
    )
  }

  private get isSaveDisabled(): ComponentStatus {
    const {workingLevelColumns, workingColumns, workingFormat} = this.state
    const {severityLevelColors, columns, severityFormat} = this.props

    const severityChanged = !_.isEqual(workingLevelColumns, severityLevelColors)
    const columnsChanged = !_.isEqual(workingColumns, columns)
    const formatChanged = !_.isEqual(workingFormat, severityFormat)

    if (severityChanged || columnsChanged || formatChanged) {
      return ComponentStatus.Default
    }

    return ComponentStatus.Disabled
  }

  private handleSave = async () => {
    const {onDismissOverlay, onSave} = this.props
    const {workingLevelColumns, workingFormat, workingColumns} = this.state

    await onSave({
      tableColumns: workingColumns,
      severityFormat: workingFormat,
      severityLevelColors: workingLevelColumns,
    })
    onDismissOverlay()
  }

  private handleResetSeverityLevels = (): void => {
    const defaults = _.map(DEFAULT_SEVERITY_LEVELS, (color, level) => {
      return {level: SeverityLevelOptions[level], color}
    })
    this.setState({workingLevelColumns: defaults})
  }

  private handleChangeSeverityLevel = (
    severityLevel: string,
    override: SeverityColor
  ): void => {
    const workingLevelColumns = this.state.workingLevelColumns.map(config => {
      if (config.level === severityLevel) {
        return {...config, color: override.name}
      }

      return config
    })

    this.setState({workingLevelColumns})
  }

  private handleChangeSeverityFormat = (format: SeverityFormat) => {
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
