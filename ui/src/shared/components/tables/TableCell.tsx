// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'
import moment from 'moment'
import classnames from 'classnames'

// Constants
import {ASCENDING} from 'src/shared/constants/tableGraph'
import {DEFAULT_TIME_FIELD} from 'src/dashboards/constants'

// Utils
import {generateThresholdsListHexs} from 'src/shared/constants/colorOperations'

// Types
import {Sort} from 'src/types/v2/dashboards'
import {TableView} from 'src/types/v2/dashboards'
import {TimeSeriesValue} from 'src/types/series'
import {CellRendererProps} from 'src/shared/components/tables/TableGraph'

interface Props extends CellRendererProps {
  sort: Sort
  data: TimeSeriesValue
  properties: TableView
  hoveredRowIndex: number
  hoveredColumnIndex: number
  isTimeVisible: boolean
  isVerticalTimeAxis: boolean
  isFirstColumnFixed: boolean
  onClickFieldName: (data: string) => void
  onHover: (e: React.MouseEvent<HTMLElement>) => void
}

export default class TableCell extends PureComponent<Props> {
  public render() {
    const {rowIndex, columnIndex, onHover} = this.props
    return (
      <div
        style={this.style}
        className={this.class}
        onClick={this.handleClick}
        data-column-index={columnIndex}
        data-row-index={rowIndex}
        onMouseOver={onHover}
        title={this.contents}
      >
        {this.contents}
      </div>
    )
  }

  private handleClick = () => {
    const {data} = this.props

    return this.isFieldName && _.isString(data)
      ? this.props.onClickFieldName(data)
      : null
  }

  private get isFieldName(): boolean {
    return this.props.isVerticalTimeAxis ? this.isFirstRow : this.isFirstCol
  }

  private get isHighlightedRow(): boolean {
    const {parent, rowIndex, hoveredRowIndex} = this.props

    return (
      rowIndex === parent.props.scrollToRow ||
      (rowIndex === hoveredRowIndex && hoveredRowIndex > 0)
    )
  }

  private get isHighlightedColumn(): boolean {
    const {columnIndex, hoveredColumnIndex} = this.props

    return columnIndex === hoveredColumnIndex && hoveredColumnIndex > 0
  }

  private get isTimeData(): boolean {
    return (
      this.props.isTimeVisible &&
      (this.props.isVerticalTimeAxis
        ? !this.isFirstRow && this.props.columnIndex === this.timeFieldIndex
        : this.props.rowIndex === this.timeFieldIndex && this.isFirstCol)
    )
  }

  private get class(): string {
    return classnames('table-graph-cell', {
      'table-graph-cell__fixed-row': this.isFixedRow,
      'table-graph-cell__fixed-column': this.isFixedColumn,
      'table-graph-cell__fixed-corner': this.isFixedCorner,
      'table-graph-cell__highlight-row': this.isHighlightedRow,
      'table-graph-cell__highlight-column': this.isHighlightedColumn,
      'table-graph-cell__numerical': this.isNumerical,
      'table-graph-cell__field-name': this.isFieldName,
      'table-graph-cell__sort-asc':
        this.isFieldName && this.isSorted && this.isAscending,
      'table-graph-cell__sort-desc':
        this.isFieldName && this.isSorted && !this.isAscending,
    })
  }

  private get isSorted(): boolean {
    const {sort, data} = this.props

    return sort.field === data
  }

  private get isAscending(): boolean {
    const {sort} = this.props

    return sort.direction === ASCENDING
  }

  private get isFirstRow(): boolean {
    const {rowIndex} = this.props

    return rowIndex === 0
  }

  private get isFirstCol(): boolean {
    const {columnIndex} = this.props

    return columnIndex === 0
  }

  private get isFixedRow(): boolean {
    return this.isFirstRow && !this.isFirstCol
  }

  private get isFixedColumn(): boolean {
    return this.props.isFirstColumnFixed && !this.isFirstRow && this.isFirstCol
  }

  private get isFixedCorner(): boolean {
    return this.isFirstRow && this.isFirstCol
  }

  private get isNumerical(): boolean {
    return !isNaN(Number.parseFloat(this.props.data as string))
  }

  private get isFixed(): boolean {
    return this.isFixedRow || this.isFixedColumn || this.isFixedCorner
  }

  private get timeFieldIndex(): number {
    const {fieldOptions} = this.props.properties

    let hiddenBeforeTime = 0
    const timeIndex = fieldOptions.findIndex(({internalName, visible}) => {
      if (!visible) {
        hiddenBeforeTime += 1
      }
      return internalName === DEFAULT_TIME_FIELD.internalName
    })

    return timeIndex - hiddenBeforeTime
  }

  private get style(): React.CSSProperties {
    const {style, properties, data} = this.props
    const {colors} = properties

    if (this.isFixed || this.isTimeData || this.isNumerical) {
      return style
    }

    const thresholdData = {colors, lastValue: data, cellType: 'table'}
    const {bgColor, textColor} = generateThresholdsListHexs(thresholdData)

    return {
      ...style,
      backgroundColor: bgColor,
      color: textColor,
    }
  }

  private get fieldName(): string {
    const {data, properties} = this.props
    const {fieldOptions = [DEFAULT_TIME_FIELD]} = properties

    const foundField =
      this.isFieldName &&
      fieldOptions.find(({internalName}) => internalName === data)

    return foundField && (foundField.displayName || foundField.internalName)
  }

  private get contents(): string {
    const {properties, data} = this.props
    const {timeFormat, decimalPlaces} = properties

    if (this.isTimeData) {
      return moment(data).format(timeFormat)
    }

    if (_.isString(data) && this.isFieldName) {
      return _.defaultTo(this.fieldName, '').toString()
    }

    if (
      _.isNumber(data) &&
      decimalPlaces.isEnforced &&
      decimalPlaces.digits < 100
    ) {
      return data.toFixed(decimalPlaces.digits)
    }

    return _.defaultTo(data, '').toString()
  }
}
