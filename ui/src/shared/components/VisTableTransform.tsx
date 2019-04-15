// Libraries
import {useMemo, FunctionComponent} from 'react'
import {Table} from '@influxdata/vis'

// Utils
import {toMinardTable} from 'src/shared/utils/toMinardTable'

// Types
import {FluxTable} from 'src/types'

interface Props {
  tables: FluxTable[]
  children: (table: Table) => JSX.Element
}

const VisTableTransform: FunctionComponent<Props> = ({tables, children}) => {
  const {table} = useMemo(() => toMinardTable(tables), [tables])

  return children(table)
}

export default VisTableTransform
