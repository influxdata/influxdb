import {createView} from 'src/shared/utils/view'
import {convertView} from 'src/shared/reducers/v2/timeMachines'

import {ViewType} from 'src/types/v2'
import {
  View,
  XYView,
  InfluxLanguage,
  QueryEditMode,
} from 'src/types/v2/dashboards'

describe('convertView', () => {
  test('should preserve view queries if they exist', () => {
    const queries = [
      {
        type: InfluxLanguage.Flux,
        text: '1 + 1',
        sourceID: '',
        editMode: QueryEditMode.Advanced,
      },
    ]

    const xyView = createView<XYView>(ViewType.XY)

    xyView.properties.queries = queries

    const convertedView = convertView(xyView, ViewType.LinePlusSingleStat)

    expect(convertedView.properties.queries).toEqual(queries)
  })

  test('should not preserve view queries if they do not exist', () => {
    const queries = [
      {
        type: InfluxLanguage.Flux,
        text: '1 + 1',
        sourceID: '',
        editMode: QueryEditMode.Advanced,
      },
    ]

    const xyView = createView<XYView>(ViewType.XY)

    xyView.properties.queries = queries

    const convertedView = convertView(xyView, ViewType.Markdown)

    expect(convertedView.properties.queries).toBeUndefined()
  })

  test('should preserve the name if it exists', () => {
    const name = 'foo'
    const xyView = createView<XYView>(ViewType.XY)

    xyView.name = name

    const convertedView = convertView(xyView, ViewType.LinePlusSingleStat)

    expect(convertedView.name).toEqual(name)
  })

  test('should preserve the id if it exists', () => {
    const xyView: View = {
      ...createView<XYView>(ViewType.XY),
      id: 'foo',
      links: {self: '123'},
    }

    const convertedView = convertView(xyView, ViewType.LinePlusSingleStat)

    expect(convertedView.id).toEqual(xyView.id)
  })

  test('should preserve the links if they exists', () => {
    const xyView: View = {
      ...createView<XYView>(ViewType.XY),
      id: 'foo',
      links: {self: '123'},
    }

    const convertedView = convertView(xyView, ViewType.LinePlusSingleStat)

    expect(convertedView.links).toEqual(xyView.links)
  })
})
