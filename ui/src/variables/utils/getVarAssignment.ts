// Utils
import {parseDuration} from 'src/shared/utils/duration'

// Types
import {VariableValues, VariableAssignment} from 'src/types'

export const getVarAssignment = (
  name: string,
  {selected, valueType}: VariableValues
): VariableAssignment => {
  const assignment = {
    type: 'VariableAssignment' as 'VariableAssignment',
    id: {type: 'Identifier' as 'Identifier', name},
  }

  switch (valueType) {
    case 'boolean':
      return {
        ...assignment,
        init: {
          type: 'BooleanLiteral',
          value: selected[0] === 'true' ? true : false,
        },
      }
    case 'unsignedLong':
      return {
        ...assignment,
        init: {
          type: 'UnsignedIntegerLiteral',
          value: Number(selected[0]),
        },
      }
    case 'long':
      return {
        ...assignment,
        init: {
          type: 'IntegerLiteral',
          value: Number(selected[0]),
        },
      }
    case 'double':
      return {
        ...assignment,
        init: {
          type: 'FloatLiteral',
          value: Number(selected[0]),
        },
      }
    case 'string':
      return {
        ...assignment,
        init: {
          type: 'StringLiteral',
          value: selected[0],
        },
      }
    case 'dateTime':
      return {
        ...assignment,
        init: {
          type: 'DateTimeLiteral',
          value: selected[0],
        },
      }
    case 'duration':
      return {
        ...assignment,
        init: {
          type: 'DurationLiteral',
          values: parseDuration(selected[0]),
        },
      }
    default:
      throw new Error(
        `cannot form VariableAssignment from value of type "${valueType}"`
      )
  }
}
