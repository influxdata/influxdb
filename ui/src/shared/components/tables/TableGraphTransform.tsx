// Libraries
import {PureComponent} from 'react'
import _ from 'lodash'
import memoizeOne from 'memoize-one'

// Utils
import {transformTableData} from 'src/dashboards/utils/tableGraph'

// Types
import {TableViewProperties, SortOptions} from 'src/types/dashboards'
import {TransformTableDataReturnType} from 'src/dashboards/utils/tableGraph'

interface Props {
  data: string[][]
  dataTypes: {[x: string]: string}
  properties: TableViewProperties
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
    const {properties, data, dataTypes, sortOptions} = this.props
    const {tableOptions, timeFormat, decimalPlaces, fieldOptions} = properties
    const fo =
      fieldOptions &&
      fieldOptions.map(opts => ({
        ...opts,
        dataType: dataTypes[opts.internalName],
      }))

    const transformedDataBundle = this.memoizedTableTransform(
      data,
      sortOptions,
      fo,
      tableOptions,
      timeFormat,
      decimalPlaces
    )

    return this.props.children(transformedDataBundle)
  }
}

export default TableGraphTransform
