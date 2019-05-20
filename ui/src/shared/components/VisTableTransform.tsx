// Libraries
import {useMemo, FunctionComponent} from 'react'
import {fluxToTable, Table} from '@influxdata/vis'
import {isSet} from 'lodash'

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

  let groupKeyUnion = []
  if (isSet(fluxGroupKeyUnion)) {
    groupKeyUnion = Array.from(fluxGroupKeyUnion)
  }

  return children({table, groupKeyUnion})
}

export default VisTableTransform
