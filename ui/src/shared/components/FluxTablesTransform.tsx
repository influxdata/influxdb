// Libraries
import {useMemo, FunctionComponent} from 'react'
import {flatMap} from 'lodash'

// Utils
import {parseResponse} from 'src/shared/parsing/flux/response'

// Types
import {FluxTable} from 'src/types'

interface Props {
  files: string[]
  children: (tables: FluxTable[]) => JSX.Element
}

const FluxTablesTransform: FunctionComponent<Props> = ({files, children}) => {
  const tables = useMemo(() => flatMap(files, parseResponse), [files])
  return children(tables)
}

export default FluxTablesTransform
