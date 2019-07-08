// Libraries
import {useMemo, FunctionComponent} from 'react'
import {fromFlux, Table} from '@influxdata/giraffe'

interface VisTableTransformResult {
  table: Table
  fluxGroupKeyUnion: string[]
}

interface Props {
  files: string[]
  children: (result: VisTableTransformResult) => JSX.Element
}

const VisTableTransform: FunctionComponent<Props> = ({files, children}) => {
  const {table, fluxGroupKeyUnion} = useMemo(
    () => fromFlux(files.join('\n\n')),
    [files]
  )

  return children({table, fluxGroupKeyUnion})
}

export default VisTableTransform
