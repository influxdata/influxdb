import {convertView, createView} from 'src/shared/utils/view'

import {ViewType} from 'src/types/v2'
import {LineView, InfluxLanguage} from 'src/types/v2/dashboards'

describe('convertView', () => {
  test('should preserve view queries if they exist', () => {
    const queries = [{type: InfluxLanguage.Flux, text: '1 + 1', source: ''}]
    const lineView = createView<LineView>(ViewType.Line)

    lineView.properties.queries = queries

    const convertedView = convertView(lineView, ViewType.Bar)

    expect(convertedView.properties.queries).toEqual(queries)
  })

  test('should not preserve view queries if they do not exist', () => {
    const queries = [{type: InfluxLanguage.Flux, text: '1 + 1', source: ''}]
    const lineView = createView<LineView>(ViewType.Line)

    lineView.properties.queries = queries

    const convertedView = convertView(lineView, ViewType.Markdown)

    expect(convertedView.properties.queries).toBeUndefined()
  })
})
