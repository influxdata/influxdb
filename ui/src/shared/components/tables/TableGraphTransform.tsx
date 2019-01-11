// Libraries
import {PureComponent} from 'react'
import _ from 'lodash'
import memoizeOne from 'memoize-one'

// Utils
import {transformTableData} from 'src/dashboards/utils/tableGraph'

// Types
import {TableView, SortOptions} from 'src/types/v2/dashboards'
import {TransformTableDataReturnType} from 'src/dashboards/utils/tableGraph'

interface Props {
  data: string[][]
  properties: TableView
  sortOptions: SortOptions
  children: (transformedDataBundle: TransformTableDataReturnType) => JSX.Element
}

const areFormatPropertiesEqual = (
  prevProperties: Props,
  newProperties: Props
) => {
  const formatProps = ['tableOptions', 'fieldOptions', 'timeFormat', 'sort']
  if (!prevProperties.properties) {
    return false
  }
  const propsEqual = formatProps.every(k =>
    _.isEqual(prevProperties.properties[k], newProperties.properties[k])
  )

  return propsEqual
}

class TableGraphTransform extends PureComponent<Props> {
  private memoizedTableTransform: typeof transformTableData = memoizeOne(
    transformTableData,
    areFormatPropertiesEqual
  )

  public render() {
    const {properties, data, sortOptions} = this.props
    const {tableOptions, timeFormat, decimalPlaces, fieldOptions} = properties

    const transformedDataBundle = this.memoizedTableTransform(
      data,
      sortOptions,
      fieldOptions,
      tableOptions,
      timeFormat,
      decimalPlaces
    )
    return this.props.children(transformedDataBundle)
  }
}

export default TableGraphTransform
