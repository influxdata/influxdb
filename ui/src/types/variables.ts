import {Variable as GenVariable} from 'src/client'
export {VariableProperties} from 'src/client'
import {Label} from 'src/types'

export interface Variable extends GenVariable {
  labels: Label[]
}
