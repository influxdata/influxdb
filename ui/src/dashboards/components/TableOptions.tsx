import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import _ from 'lodash'

import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import GraphOptionsTimeFormat from 'src/dashboards/components/GraphOptionsTimeFormat'
import GraphOptionsTimeAxis from 'src/dashboards/components/GraphOptionsTimeAxis'
import GraphOptionsSortBy from 'src/dashboards/components/GraphOptionsSortBy'
import GraphOptionsTextWrapping from 'src/dashboards/components/GraphOptionsTextWrapping'
import GraphOptionsFixFirstColumn from 'src/dashboards/components/GraphOptionsFixFirstColumn'
import GraphOptionsCustomizeFields from 'src/dashboards/components/GraphOptionsCustomizeFields'

import ThresholdsList from 'src/shared/components/ThresholdsList'
import ThresholdsListTypeToggle from 'src/shared/components/ThresholdsListTypeToggle'

import {TIME_FIELD_DEFAULT} from 'src/shared/constants/tableGraph'
import {updateTableOptions} from 'src/dashboards/actions/cellEditorOverlay'

type RenamableField = {
  internalName: string
  displayName: string
  visible: boolean
}

type Options = {
  timeFormat: string
  verticalTimeAxis: boolean
  sortBy: RenamableField
  wrapping: string
  fieldNames: RenamableField[]
  fixFirstColumn: boolean
}

interface QueryConfig {
  measurement: string
  fields: {
    alias: string
    value: string
  }[]
}

interface Props {
  queryConfigs: QueryConfig[]
  handleUpdateTableOptions: (options: Options) => void
  tableOptions: Options
  onResetFocus: () => void
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
    const {queryConfigs} = this.props

    const queryFields = _.flatten(
      queryConfigs.map(({measurement, fields}) => {
        return fields.map(({alias}) => {
          const internalName = `${measurement}.${alias}`
          const existing = this.fieldNames.find(
            f => f.internalName === internalName
          )
          return existing || {internalName, displayName: '', visible: true}
        })
      })
    )

    return [this.timeColumn, ...queryFields]
  }

  componentWillMount() {
    const {handleUpdateTableOptions, tableOptions} = this.props
    handleUpdateTableOptions({
      ...tableOptions,
      fieldNames: this.computedFieldNames,
    })
  }

  handleChooseSortBy = option => {
    const {tableOptions, handleUpdateTableOptions} = this.props
    const sortBy = {
      displayName: option.text,
      internalName: option.key,
      visible: true,
    }

    handleUpdateTableOptions({...tableOptions, sortBy})
  }

  handleTimeFormatChange = timeFormat => {
    const {tableOptions, handleUpdateTableOptions} = this.props
    handleUpdateTableOptions({...tableOptions, timeFormat})
  }

  onToggleVerticalTimeAxis = verticalTimeAxis => () => {
    const {tableOptions, handleUpdateTableOptions} = this.props
    handleUpdateTableOptions({...tableOptions, verticalTimeAxis})
  }

  handleToggleTextWrapping = () => {}

  handleToggleFixFirstColumn = () => {
    const {handleUpdateTableOptions, tableOptions} = this.props
    const fixFirstColumn = !tableOptions.fixFirstColumn
    handleUpdateTableOptions({...tableOptions, fixFirstColumn})
  }

  handleFieldUpdate = field => {
    const {handleUpdateTableOptions, tableOptions} = this.props
    const {fieldNames} = tableOptions
    const updatedFields = fieldNames.map(
      f => (f.internalName === field.internalName ? field : f)
    )
    handleUpdateTableOptions({...tableOptions, fieldNames: updatedFields})
  }

  render() {
    const {
      tableOptions: {timeFormat, fieldNames, verticalTimeAxis, fixFirstColumn},
      onResetFocus,
      tableOptions,
    } = this.props

    const tableSortByOptions = this.computedFieldNames.map(col => ({
      text: col.displayName || col.internalName,
      key: col.internalName,
    }))

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
              onToggleVerticalTimeAxis={this.onToggleVerticalTimeAxis}
            />
            <GraphOptionsSortBy
              selected={tableOptions.sortBy || TIME_FIELD_DEFAULT}
              sortByOptions={tableSortByOptions}
              onChooseSortBy={this.handleChooseSortBy}
            />
            <GraphOptionsTextWrapping
              thresholdsListType="background"
              onToggleTextWrapping={this.handleToggleTextWrapping}
            />
            <GraphOptionsFixFirstColumn
              fixed={fixFirstColumn}
              onToggleFixFirstColumn={this.handleToggleFixFirstColumn}
            />
          </div>
          <GraphOptionsCustomizeFields
            fields={fieldNames}
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
