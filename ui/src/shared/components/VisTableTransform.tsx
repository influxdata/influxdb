// Libraries
import {useMemo, FunctionComponent} from 'react'
import {fluxToTable, Table} from '@influxdata/vis'

interface Props {
  files: string[]
  children: (table: Table) => JSX.Element
}

const VisTableTransform: FunctionComponent<Props> = ({files, children}) => {
  const {table} = useMemo(() => fluxToTable(files.join('\n\n')), [files])

  return children(table)
}

export default VisTableTransform
