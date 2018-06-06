import {TEMPLATE_VARIABLE_QUERIES} from 'src/dashboards/constants'
import {Template, TemplateQuery} from 'src/types/dashboard'

interface PartialTemplateWithQuery {
  query: string
  tempVars: Array<Partial<Template>>
}

const generateTemplateVariableQuery = ({
  type,
  query: {
    database,
    // rp, TODO
    measurement,
    tagKey,
  },
}: Partial<Template>): PartialTemplateWithQuery => {
  const tempVars = []

  if (database) {
    tempVars.push({
      tempVar: ':database:',
      values: [
        {
          type: 'database',
          value: database,
        },
      ],
    })
  }
  if (measurement) {
    tempVars.push({
      tempVar: ':measurement:',
      values: [
        {
          type: 'measurement',
          value: measurement,
        },
      ],
    })
  }
  if (tagKey) {
    tempVars.push({
      tempVar: ':tagKey:',
      values: [
        {
          type: 'tagKey',
          value: tagKey,
        },
      ],
    })
  }

  const query: string = TEMPLATE_VARIABLE_QUERIES[type]

  return {
    query,
    tempVars,
  }
}

export const makeQueryForTemplate = ({
  influxql,
  db,
  measurement,
  tagKey,
}: TemplateQuery): string =>
  influxql
    .replace(':database:', `"${db}"`)
    .replace(':measurement:', `"${measurement}"`)
    .replace(':tagKey:', `"${tagKey}"`)

export default generateTemplateVariableQuery
