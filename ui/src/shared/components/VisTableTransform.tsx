// Libraries
import {useMemo, FunctionComponent} from 'react'
import {fluxToTable, Table} from '@influxdata/vis'

interface VisTableTransformResult {
  table: Table
  groupKeyUnion: Array<string>
}

interface Props {
  files: string[]
  children: (result: VisTableTransformResult) => JSX.Element
}

const VisTableTransform: FunctionComponent<Props> = ({files, children}) => {
  const {table, fluxGroupKeyUnion} = useMemo(
    () => fluxToTable(files.join('\n\n')),
    [files]
  )

  const groupKeyUnion = Array.from(fluxGroupKeyUnion)

  return children({table, groupKeyUnion})
}

export default VisTableTransform
