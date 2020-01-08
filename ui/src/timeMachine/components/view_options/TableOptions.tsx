// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'

// Components
import DecimalPlacesOption from 'src/timeMachine/components/view_options/DecimalPlaces'
import ColumnOptions from 'src/shared/components/columns_options/ColumnsOptions'
import FixFirstColumn from 'src/timeMachine/components/view_options/FixFirstColumn'
import TimeFormat from 'src/timeMachine/components/view_options/TimeFormat'
import SortBy from 'src/timeMachine/components/view_options/SortBy'
import {Grid, Form} from '@influxdata/clockface'
import ThresholdsSettings from 'src/shared/components/ThresholdsSettings'
import ThresholdColoring from 'src/timeMachine/components/view_options/ThresholdColoring'

// Constants

// Actions
import {
  setDecimalPlaces,
  setColors,
  setFieldOptions,
  updateFieldOption,
  setTableOptions,
  setTimeFormat,
} from 'src/timeMachine/actions'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {
  AppState,
  NewView,
  DecimalPlaces,
  TableViewProperties,
  FieldOption,
  TableOptions as ViewTableOptions,
  Color,
} from 'src/types'
import {move} from 'src/shared/utils/move'

interface StateProps {
  colors: Color[]
  timeFormat: string
  fieldOptions: FieldOption[]
  decimalPlaces: DecimalPlaces
  tableOptions: ViewTableOptions
}

interface DispatchProps {
  onSetColors: typeof setColors
  onSetTimeFormat: typeof setTimeFormat
  onSetFieldOptions: typeof setFieldOptions
  onUpdateFieldOption: typeof updateFieldOption
  onSetTableOptions: typeof setTableOptions
  onSetDecimalPlaces: typeof setDecimalPlaces
}

type Props = DispatchProps & StateProps

@ErrorHandling
export class TableOptions extends Component<Props, {}> {
  public render() {
    const {
      timeFormat,
      onSetColors,
      fieldOptions,
      tableOptions,
      colors,
      decimalPlaces,
      onSetTimeFormat,
      onSetDecimalPlaces,
    } = this.props

    const filteredColumns = this.filterFieldOptions(fieldOptions)

    const {fixFirstColumn, sortBy} = tableOptions

    return (
      <>
        <Grid.Column>
          <h4 className="view-options--header">Customize Table</h4>
        </Grid.Column>
        {!!fieldOptions.length && (
          <SortBy
            selected={sortBy}
            fieldOptions={fieldOptions}
            onChange={this.handleChangeSortBy}
          />
        )}
        <Grid.Column>
          <Form.Element label="Time Format">
            <TimeFormat
              timeFormat={timeFormat}
              onTimeFormatChange={onSetTimeFormat}
            />
          </Form.Element>
        </Grid.Column>
        {decimalPlaces && (
          <DecimalPlacesOption
            digits={decimalPlaces.digits}
            isEnforced={decimalPlaces.isEnforced}
            onDecimalPlacesChange={onSetDecimalPlaces}
          />
        )}
        <Grid.Column>
          <h4 className="view-options--header">Column Settings</h4>
        </Grid.Column>
        {/* TODO (watts): this currently doesn't working removing for alpha.
          <TimeAxis
          verticalTimeAxis={verticalTimeAxis}
          onToggleVerticalTimeAxis={this.handleToggleVerticalTimeAxis}
        /> */}
        <FixFirstColumn
          fixed={fixFirstColumn}
          onToggleFixFirstColumn={this.handleToggleFixFirstColumn}
        />
        <ColumnOptions
          columns={filteredColumns}
          onMoveColumn={this.handleMoveColumn}
          onUpdateColumn={this.handleUpdateColumn}
        />
        <Grid.Column>
          <h4 className="view-options--header">Colorized Thresholds</h4>
        </Grid.Column>
        <Grid.Column>
          <ThresholdsSettings
            thresholds={colors}
            onSetThresholds={onSetColors}
          />
        </Grid.Column>
        <Grid.Column>
          <ThresholdColoring />
        </Grid.Column>
      </>
    )
  }

  private filterFieldOptions = (fieldOptions: FieldOption[]) => {
    return fieldOptions.filter(
      col =>
        col.internalName !== 'time' &&
        col.internalName !== '' &&
        col.internalName !== 'result' &&
        col.internalName !== 'table'
    )
  }

  private handleChangeSortBy = (sortBy: FieldOption) => {
    const {tableOptions, onSetTableOptions} = this.props
    onSetTableOptions({...tableOptions, sortBy})
  }

  private handleMoveColumn = (dragIndex: number, hoverIndex: number) => {
    const fieldOptions = move(
      this.filterFieldOptions(this.props.fieldOptions),
      dragIndex,
      hoverIndex
    )
    this.props.onSetFieldOptions(fieldOptions)
  }

  private handleUpdateColumn = (fieldOption: FieldOption) => {
    this.props.onUpdateFieldOption(fieldOption)
  }

  private handleToggleFixFirstColumn = () => {
    const {onSetTableOptions, tableOptions} = this.props
    const fixFirstColumn = !tableOptions.fixFirstColumn
    onSetTableOptions({...tableOptions, fixFirstColumn})
  }
}

const mstp = (state: AppState) => {
  const view = getActiveTimeMachine(state).view as NewView<TableViewProperties>
  const {
    colors,
    decimalPlaces,
    fieldOptions,
    tableOptions,
    timeFormat,
  } = view.properties

  return {colors, decimalPlaces, fieldOptions, tableOptions, timeFormat}
}

const mdtp: DispatchProps = {
  onSetDecimalPlaces: setDecimalPlaces,
  onSetColors: setColors,
  onSetFieldOptions: setFieldOptions,
  onUpdateFieldOption: updateFieldOption,
  onSetTableOptions: setTableOptions,
  onSetTimeFormat: setTimeFormat,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(TableOptions)
