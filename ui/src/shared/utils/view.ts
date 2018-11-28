import {get, cloneDeep} from 'lodash'

import {View, ViewType, ViewShape} from 'src/types/v2'
import {
  XYView,
  XYViewGeom,
  LinePlusSingleStatView,
  SingleStatView,
  TableView,
  GaugeView,
  MarkdownView,
  NewView,
  ViewProperties,
  DashboardQuery,
  InfluxLanguage,
} from 'src/types/v2/dashboards'
import {DEFAULT_GAUGE_COLORS} from 'src/shared/constants/thresholds'

function defaultView() {
  return {
    name: 'Untitled',
  }
}

function defaultViewQueries(): DashboardQuery[] {
  return [
    {
      text: '',
      type: InfluxLanguage.Flux,
      sourceID: '',
    },
  ]
}

function defaultLineViewProperties() {
  return {
    queries: defaultViewQueries(),
    colors: [],
    legend: {},
    note: '',
    showNoteWhenEmpty: false,
    axes: {
      x: {
        bounds: ['', ''] as [string, string],
        label: '',
        prefix: '',
        suffix: '',
        base: '10',
        scale: 'linear',
      },
      y: {
        bounds: ['', ''] as [string, string],
        label: '',
        prefix: '',
        suffix: '',
        base: '10',
        scale: 'linear',
      },
      y2: {
        bounds: ['', ''] as [string, string],
        label: '',
        prefix: '',
        suffix: '',
        base: '10',
        scale: 'linear',
      },
    },
  }
}

function defaultGaugeViewProperties() {
  return {
    queries: defaultViewQueries(),
    colors: DEFAULT_GAUGE_COLORS,
    prefix: '',
    suffix: '',
    note: '',
    showNoteWhenEmpty: false,
    decimalPlaces: {
      isEnforced: true,
      digits: 2,
    },
  }
}

// Defines the zero values of the various view types
const NEW_VIEW_CREATORS = {
  [ViewType.XY]: (): NewView<XYView> => ({
    ...defaultView(),
    properties: {
      ...defaultLineViewProperties(),
      type: ViewType.XY,
      shape: ViewShape.ChronografV2,
      geom: XYViewGeom.Line,
    },
  }),
  [ViewType.SingleStat]: (): NewView<SingleStatView> => ({
    ...defaultView(),
    properties: {
      ...defaultGaugeViewProperties(),
      type: ViewType.SingleStat,
      shape: ViewShape.ChronografV2,
    },
  }),
  [ViewType.Gauge]: (): NewView<GaugeView> => ({
    ...defaultView(),
    properties: {
      ...defaultGaugeViewProperties(),
      type: ViewType.Gauge,
      shape: ViewShape.ChronografV2,
    },
  }),
  [ViewType.LinePlusSingleStat]: (): NewView<LinePlusSingleStatView> => ({
    ...defaultView(),
    properties: {
      ...defaultLineViewProperties(),
      ...defaultGaugeViewProperties(),
      type: ViewType.LinePlusSingleStat,
      shape: ViewShape.ChronografV2,
    },
  }),
  [ViewType.Table]: (): NewView<TableView> => ({
    ...defaultView(),
    properties: {
      type: ViewType.Table,
      shape: ViewShape.ChronografV2,
      queries: defaultViewQueries(),
      colors: [],
      tableOptions: {
        verticalTimeAxis: false,
        sortBy: {
          internalName: '',
          displayName: '',
          visible: false,
        },
        fixFirstColumn: false,
      },
      fieldOptions: [],
      decimalPlaces: {
        isEnforced: false,
        digits: 2,
      },
      timeFormat: 'YYYY-MM-DD HH:mm:ss',
      note: '',
      showNoteWhenEmpty: false,
    },
  }),
  [ViewType.Markdown]: (): NewView<MarkdownView> => ({
    ...defaultView(),
    properties: {
      type: ViewType.Markdown,
      shape: ViewShape.ChronografV2,
      note: '',
    },
  }),
}

export function createView<T extends ViewProperties = ViewProperties>(
  viewType: ViewType = ViewType.XY
): NewView<T> {
  const creator = NEW_VIEW_CREATORS[viewType]

  if (!creator) {
    throw new Error(`no view creator implemented for view of type ${viewType}`)
  }

  return creator()
}

export function convertView<T extends View | NewView>(
  view: T,
  outType: ViewType
): T {
  const viewCreator = NEW_VIEW_CREATORS[outType]

  if (!viewCreator) {
    throw new Error(`no view creator exists for type ${outType}`)
  }

  const newView: any = viewCreator()

  const oldViewQueries = get(view, 'properties.queries')
  const newViewQueries = get(newView, 'properties.queries')

  if (oldViewQueries && newViewQueries) {
    newView.properties.queries = cloneDeep(oldViewQueries)
  }

  newView.name = view.name
  newView.id = (view as any).id
  newView.links = (view as any).links

  return newView
}

// Replaces the text of the first query in a view, inserting a query if none exist
export function replaceQuery<T extends View | NewView>(view: T, text): T {
  const anyView: any = view
  const queries = anyView.properties.queries

  if (!queries) {
    return view
  }

  if (!queries[0]) {
    const query: DashboardQuery = {
      text,
      type: InfluxLanguage.Flux,
      sourceID: '',
    }

    return {
      ...anyView,
      properties: {
        ...anyView.properties,
        queries: [query],
      },
    }
  }

  const newQueries = queries.map((query: DashboardQuery, i) => {
    if (i !== 0) {
      return query
    }

    return {...query, text}
  })

  return {
    ...anyView,
    properties: {
      ...anyView.properties,
      queries: newQueries,
    },
  }
}
