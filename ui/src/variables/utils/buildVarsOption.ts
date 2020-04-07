// Constants
import {OPTION_NAME} from 'src/variables/constants'

// Types
import {File, Property} from 'src/types/ast'
import {VariableAssignment} from 'src/types/ast'

export const buildVarsOption = (variables: VariableAssignment[]): File => ({
  type: 'File',
  package: null,
  imports: null,
  body: [
    {
      type: 'OptionStatement',
      assignment: {
        type: 'VariableAssignment',
        id: {
          type: 'Identifier',
          name: OPTION_NAME,
        },
        init: {
          type: 'ObjectExpression',
          properties: variables.filter(v => !!v).map(assignmentToProperty),
        },
      },
    },
  ],
})

const assignmentToProperty = (variable: VariableAssignment): Property => {
  return {
    type: 'Property',
    key: variable.id,
    value: variable.init,
  }
}
