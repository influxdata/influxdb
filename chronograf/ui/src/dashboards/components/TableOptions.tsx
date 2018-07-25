import React, {Component} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import GraphOptionsCustomizeFields from 'src/dashboards/components/GraphOptionsCustomizeFields'
import GraphOptionsFixFirstColumn from 'src/dashboards/components/GraphOptionsFixFirstColumn'
import GraphOptionsSortBy from 'src/dashboards/components/GraphOptionsSortBy'
import GraphOptionsTimeAxis from 'src/dashboards/components/GraphOptionsTimeAxis'
import GraphOptionsTimeFormat from 'src/dashboards/components/GraphOptionsTimeFormat'
import GraphOptionsDecimalPlaces from 'src/dashboards/components/GraphOptionsDecimalPlaces'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'

import _ from 'lodash'

import ThresholdsList from 'src/shared/components/ThresholdsList'
import ThresholdsListTypeToggle from 'src/shared/components/ThresholdsListTypeToggle'

import {
  updateTableOptions,
  updateFieldOptions,
  changeTimeFormat,
  changeDecimalPlaces,
} from 'src/dashboards/actions/cellEditorOverlay'
import {DEFAULT_TIME_FIELD} from 'src/dashboards/constants'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {DecimalPlaces} from 'src/types/dashboards'
import {QueryConfig} from 'src/types/queries'

interface DropdownOption {
  text: string
  key: string
}

interface RenamableField {
  internalName: string
  displayName: string
  visible: boolean
}

interface TableOptionsInterface {
  verticalTimeAxis: boolean
  sortBy: RenamableField
  fixFirstColumn: boolean
}

interface Props {
  queryConfigs: QueryConfig[]
  handleUpdateTableOptions: (options: TableOptionsInterface) => void
  handleUpdateFieldOptions: (fieldOptions: RenamableField[]) => void
  handleChangeTimeFormat: (timeFormat: string) => void
  handleChangeDecimalPlaces: (decimalPlaces: number) => void
  tableOptions: TableOptionsInterface
  fieldOptions: RenamableField[]
  timeFormat: string
  decimalPlaces: DecimalPlaces
  onResetFocus: () => void
}

@ErrorHandling
export class TableOptions extends Component<Props, {}> {
  constructor(props) {
    super(props)
    this.moveField = this.moveField.bind(this)
  }

  public render() {
    const {
      tableOptions: {verticalTimeAxis, fixFirstColumn},
      fieldOptions,
      timeFormat,
      onResetFocus,
      tableOptions,
      decimalPlaces,
    } = this.props

    const tableSortByOptions = fieldOptions.map(field => ({
      key: field.internalName,
      text: field.displayName || field.internalName,
    }))

    return (
      <FancyScrollbar
        className="display-options--cell y-axis-controls"
        autoHide={false}
      >
        <div className="display-options--cell-wrapper">
          <h5 className="display-options--header">Table Controls</h5>
          <div className="form-group-wrapper">
            <GraphOptionsSortBy
              selected={tableOptions.sortBy || DEFAULT_TIME_FIELD}
              sortByOptions={tableSortByOptions}
              onChooseSortBy={this.handleChooseSortBy}
            />
            <GraphOptionsDecimalPlaces
              digits={decimalPlaces.digits}
              isEnforced={decimalPlaces.isEnforced}
              onDecimalPlacesChange={this.handleDecimalPlacesChange}
            />
            <GraphOptionsTimeAxis
              verticalTimeAxis={verticalTimeAxis}
              onToggleVerticalTimeAxis={this.handleToggleVerticalTimeAxis}
            />
            <GraphOptionsTimeFormat
              timeFormat={timeFormat}
              onTimeFormatChange={this.handleTimeFormatChange}
            />
            <GraphOptionsFixFirstColumn
              fixed={fixFirstColumn}
              onToggleFixFirstColumn={this.handleToggleFixFirstColumn}
            />
          </div>
          <GraphOptionsCustomizeFields
            fields={fieldOptions}
            onFieldUpdate={this.handleFieldUpdate}
            moveField={this.moveField}
          />
          <ThresholdsList showListHeading={true} onResetFocus={onResetFocus} />
          <div className="form-group-wrapper graph-options-group">
            <ThresholdsListTypeToggle containerClass="form-group col-xs-6" />
          </div>
        </div>
      </FancyScrollbar>
    )
  }

  private moveField(dragIndex, hoverIndex) {
    const {handleUpdateFieldOptions, fieldOptions} = this.props

    const draggedField = fieldOptions[dragIndex]

    const fieldOptionsRemoved = _.concat(
      _.slice(fieldOptions, 0, dragIndex),
      _.slice(fieldOptions, dragIndex + 1)
    )

    const fieldOptionsAdded = _.concat(
      _.slice(fieldOptionsRemoved, 0, hoverIndex),
      [draggedField],
      _.slice(fieldOptionsRemoved, hoverIndex)
    )

    handleUpdateFieldOptions(fieldOptionsAdded)
  }

  private handleChooseSortBy = (option: DropdownOption) => {
    const {tableOptions, handleUpdateTableOptions, fieldOptions} = this.props
    const sortBy = fieldOptions.find(f => f.internalName === option.key)
    handleUpdateTableOptions({...tableOptions, sortBy})
  }

  private handleTimeFormatChange = timeFormat => {
    const {handleChangeTimeFormat} = this.props
    handleChangeTimeFormat(timeFormat)
  }

  private handleDecimalPlacesChange = decimalPlaces => {
    const {handleChangeDecimalPlaces} = this.props
    handleChangeDecimalPlaces(decimalPlaces)
  }

  private handleToggleVerticalTimeAxis = verticalTimeAxis => () => {
    const {tableOptions, handleUpdateTableOptions} = this.props
    handleUpdateTableOptions({...tableOptions, verticalTimeAxis})
  }

  private handleToggleFixFirstColumn = () => {
    const {handleUpdateTableOptions, tableOptions} = this.props
    const fixFirstColumn = !tableOptions.fixFirstColumn
    handleUpdateTableOptions({...tableOptions, fixFirstColumn})
  }

  private handleFieldUpdate = field => {
    const {
      handleUpdateTableOptions,
      handleUpdateFieldOptions,
      tableOptions,
      fieldOptions,
    } = this.props
    const {sortBy} = tableOptions

    const updatedFieldOptions = fieldOptions.map(
      f => (f.internalName === field.internalName ? field : f)
    )

    if (sortBy.internalName === field.internalName) {
      const updatedSortBy = {...sortBy, displayName: field.displayName}
      handleUpdateTableOptions({
        ...tableOptions,
        sortBy: updatedSortBy,
      })
    }

    handleUpdateFieldOptions(updatedFieldOptions)
  }
}

const mapStateToProps = ({
  cellEditorOverlay: {
    cell: {tableOptions, timeFormat, fieldOptions, decimalPlaces},
  },
}) => ({
  tableOptions,
  timeFormat,
  fieldOptions,
  decimalPlaces,
})

const mapDispatchToProps = dispatch => ({
  handleUpdateTableOptions: bindActionCreators(updateTableOptions, dispatch),
  handleUpdateFieldOptions: bindActionCreators(updateFieldOptions, dispatch),
  handleChangeTimeFormat: bindActionCreators(changeTimeFormat, dispatch),
  handleChangeDecimalPlaces: bindActionCreators(changeDecimalPlaces, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(TableOptions)
