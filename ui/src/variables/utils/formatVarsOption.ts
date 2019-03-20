import {OPTION_NAME} from 'src/variables/constants/index'
import {formatExpression} from 'src/variables/utils/formatExpression'
import {VariableAssignment} from 'src/types/ast'

export const formatVarsOption = (variables: VariableAssignment[]): string => {
  if (!variables.length) {
    return ''
  }

  const lines = variables.map(v => `${v.id.name}: ${formatExpression(v.init)}`)

  const option = `option ${OPTION_NAME} = {
  ${lines.join(',\n  ')}
}`

  return option
}
