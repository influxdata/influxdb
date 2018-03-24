import React, {SFC} from 'react'
import FuncsButton from 'src/ifql/components/FuncsButton'

interface Props {
  funcs: string[]
}

const TimeMachine: SFC<Props> = ({funcs}) => {
  return <FuncsButton funcs={funcs} />
}

export default TimeMachine
