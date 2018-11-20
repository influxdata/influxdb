// Libraries
import React, {SFC} from 'react'

// Components
import ToolbarFunction from 'src/shared/components/flux_functions_toolbar/ToolbarFunction'

// Types
import {FluxToolbarFunction} from 'src/types/shared'

interface Props {
  category: string
  funcs: FluxToolbarFunction[]
  onClickFunction: (s: string) => void
}

const FunctionCategory: SFC<Props> = props => {
  const {category, funcs, onClickFunction} = props

  return (
    <dl className="flux-functions-toolbar--category">
      <dt>{category}</dt>
      {funcs.map(func => (
        <ToolbarFunction
          onClickFunction={onClickFunction}
          key={func.name}
          func={func}
        />
      ))}
    </dl>
  )
}

export default FunctionCategory
