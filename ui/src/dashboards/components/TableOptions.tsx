import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import GraphOptionsCustomizeFields from 'src/dashboards/components/GraphOptionsCustomizeFields'
import GraphOptionsFixFirstColumn from 'src/dashboards/components/GraphOptionsFixFirstColumn'
import GraphOptionsSortBy from 'src/dashboards/components/GraphOptionsSortBy'
import GraphOptionsTimeAxis from 'src/dashboards/components/GraphOptionsTimeAxis'
import GraphOptionsTimeFormat from 'src/dashboards/components/GraphOptionsTimeFormat'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'

import _ from 'lodash'

import ThresholdsList from 'src/shared/components/ThresholdsList'
import ThresholdsListTypeToggle from 'src/shared/components/ThresholdsListTypeToggle'

import {updateTableOptions} from 'src/dashboards/actions/cellEditorOverlay'
import {TIME_FIELD_DEFAULT} from 'src/shared/constants/tableGraph'

interface Option {
  text: string
  key: string
}

interface RenamableField {
  internalName: string
  displayName: string
  visible: boolean
}

interface Options {
  timeFormat: string
  verticalTimeAxis: boolean
  sortBy: RenamableField
  fieldNames: RenamableField[]
  fixFirstColumn: boolean
}

interface Props {
  handleUpdateTableOptions: (options: Options) => void
  tableOptions: Options
  onResetFocus: () => void
  dataLabels: string[]
}

export class TableOptions extends PureComponent<Props, {}> {
  constructor(props) {
    super(props)
  }

  get fieldNames() {
    const {tableOptions: {fieldNames}} = this.props
    return fieldNames || []
  }

  get timeColumn() {
    return (
      this.fieldNames.find(f => f.internalName === 'time') || TIME_FIELD_DEFAULT
    )
  }

  get computedFieldNames() {
    const {dataLabels} = this.props

    return dataLabels.length
      ? dataLabels.map(label => {
          const existing = this.fieldNames.find(f => f.internalName === label)
          return (
            existing || {internalName: label, displayName: '', visible: true}
          )
        })
      : [this.timeColumn]
  }

  public handleChooseSortBy = (option: Option) => {
    const {tableOptions, handleUpdateTableOptions} = this.props
    const sortBy = {
      displayName: option.text === option.key ? '' : option.text,
      internalName: option.key,
      visible: true,
    }

    handleUpdateTableOptions({...tableOptions, sortBy})
  }

  public handleTimeFormatChange = timeFormat => {
    const {tableOptions, handleUpdateTableOptions} = this.props
    handleUpdateTableOptions({...tableOptions, timeFormat})
  }

  public handleToggleVerticalTimeAxis = verticalTimeAxis => () => {
    const {tableOptions, handleUpdateTableOptions} = this.props
    handleUpdateTableOptions({...tableOptions, verticalTimeAxis})
  }

  public handleToggleFixFirstColumn = () => {
    const {handleUpdateTableOptions, tableOptions} = this.props
    const fixFirstColumn = !tableOptions.fixFirstColumn
    handleUpdateTableOptions({...tableOptions, fixFirstColumn})
  }

  public handleFieldUpdate = field => {
    const {handleUpdateTableOptions, tableOptions, dataLabels} = this.props
    const {sortBy, fieldNames} = tableOptions
    const fields =
      fieldNames.length >= dataLabels.length
        ? fieldNames
        : this.computedFieldNames
    const updatedFields = fields.map(
      f => (f.internalName === field.internalName ? field : f)
    )
    const updatedSortBy =
      sortBy.internalName === field.internalName
        ? {...sortBy, displayName: field.displayName}
        : sortBy

    handleUpdateTableOptions({
      ...tableOptions,
      fieldNames: updatedFields,
      sortBy: updatedSortBy,
    })
  }

  public componentWillMount() {
    const {handleUpdateTableOptions, tableOptions} = this.props
    handleUpdateTableOptions({
      ...tableOptions,
      fieldNames: this.computedFieldNames,
    })
  }

  public shouldComponentUpdate(nextProps) {
    const {tableOptions, dataLabels} = this.props
    const tableOptionsDifferent = !_.isEqual(
      tableOptions,
      nextProps.tableOptions
    )
    const dataLabelsDifferent = !_.isEqual(dataLabels, nextProps.dataLabels)

    return tableOptionsDifferent || dataLabelsDifferent
  }

  public handleToggleTextWrapping = () => {}

  public render() {
    const {
      tableOptions: {timeFormat, fieldNames, verticalTimeAxis, fixFirstColumn},
      onResetFocus,
      tableOptions,
    } = this.props

    const tableSortByOptions = this.computedFieldNames.map(field => ({
      key: field.internalName,
      text: field.displayName || field.internalName,
    }))

    const fields = fieldNames.length > 1 ? fieldNames : this.computedFieldNames

    return (
      <FancyScrollbar
        className="display-options--cell y-axis-controls"
        autoHide={false}
      >
        <div className="display-options--cell-wrapper">
          <h5 className="display-options--header">Table Controls</h5>
          <div className="form-group-wrapper">
            <GraphOptionsTimeFormat
              timeFormat={timeFormat}
              onTimeFormatChange={this.handleTimeFormatChange}
            />
            <GraphOptionsTimeAxis
              verticalTimeAxis={verticalTimeAxis}
              onToggleVerticalTimeAxis={this.handleToggleVerticalTimeAxis}
            />
            <GraphOptionsSortBy
              selected={tableOptions.sortBy || TIME_FIELD_DEFAULT}
              sortByOptions={tableSortByOptions}
              onChooseSortBy={this.handleChooseSortBy}
            />
            <GraphOptionsFixFirstColumn
              fixed={fixFirstColumn}
              onToggleFixFirstColumn={this.handleToggleFixFirstColumn}
            />
          </div>
          <GraphOptionsCustomizeFields
            fields={fields}
            onFieldUpdate={this.handleFieldUpdate}
          />
          <ThresholdsList showListHeading={true} onResetFocus={onResetFocus} />
          <div className="form-group-wrapper graph-options-group">
            <ThresholdsListTypeToggle containerClass="form-group col-xs-6" />
          </div>
        </div>
      </FancyScrollbar>
    )
  }
}

const mapStateToProps = ({cellEditorOverlay: {cell: {tableOptions}}}) => ({
  tableOptions,
})

const mapDispatchToProps = dispatch => ({
  handleUpdateTableOptions: bindActionCreators(updateTableOptions, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(TableOptions)
