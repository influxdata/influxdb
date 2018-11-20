import {convertView, createView} from 'src/shared/utils/view'

import {ViewType} from 'src/types/v2'
import {View, LineView, InfluxLanguage} from 'src/types/v2/dashboards'

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

  test('should preserve the name if it exists', () => {
    const name = 'foo'
    const lineView = createView<LineView>(ViewType.Line)

    lineView.name = name

    const convertedView = convertView(lineView, ViewType.Bar)

    expect(convertedView.name).toEqual(name)
  })

  test('should preserve the id if it exists', () => {
    const lineView: View = {
      ...createView<LineView>(ViewType.Line),
      id: 'foo',
      links: {self: '123'},
    }

    const convertedView = convertView(lineView, ViewType.Bar)

    expect(convertedView.id).toEqual(lineView.id)
  })

  test('should preserve the links if they exists', () => {
    const lineView: View = {
      ...createView<LineView>(ViewType.Line),
      id: 'foo',
      links: {self: '123'},
    }

    const convertedView = convertView(lineView, ViewType.Bar)

    expect(convertedView.links).toEqual(lineView.links)
  })
})
