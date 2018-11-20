import {get, cloneDeep} from 'lodash'

import {View, ViewType, ViewShape} from 'src/types/v2'
import {
  LineView,
  BarChartView,
  StepPlotView,
  StackedView,
  LinePlusSingleStatView,
  SingleStatView,
  TableView,
  GaugeView,
  MarkdownView,
  NewView,
  ViewProperties,
  InfluxLanguage,
} from 'src/types/v2/dashboards'

function defaultView() {
  return {
    name: 'Untitled',
  }
}

function defaultViewQueries() {
  return []
}

function defaultLineViewProperties() {
  return {
    queries: defaultViewQueries(),
    colors: [],
    legend: {},
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
    colors: [],
    prefix: '',
    suffix: '',
    decimalPlaces: {
      isEnforced: true,
      digits: 2,
    },
  }
}

// Defines the zero values of the various view types
const NEW_VIEW_CREATORS = {
  [ViewType.Bar]: (): NewView<BarChartView> => ({
    ...defaultView(),
    properties: {
      ...defaultLineViewProperties(),
      type: ViewType.Bar,
      shape: ViewShape.ChronografV2,
    },
  }),
  [ViewType.Line]: (): NewView<LineView> => ({
    ...defaultView(),
    properties: {
      ...defaultLineViewProperties(),
      type: ViewType.Line,
      shape: ViewShape.ChronografV2,
    },
  }),
  [ViewType.Stacked]: (): NewView<StackedView> => ({
    ...defaultView(),
    properties: {
      ...defaultLineViewProperties(),
      type: ViewType.Stacked,
      shape: ViewShape.ChronografV2,
    },
  }),
  [ViewType.StepPlot]: (): NewView<StepPlotView> => ({
    ...defaultView(),
    properties: {
      ...defaultLineViewProperties(),
      type: ViewType.StepPlot,
      shape: ViewShape.ChronografV2,
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
    },
  }),
  [ViewType.Markdown]: (): NewView<MarkdownView> => ({
    ...defaultView(),
    properties: {
      type: ViewType.Markdown,
      shape: ViewShape.ChronografV2,
      text: '',
    },
  }),
}

export function createView<T extends ViewProperties = ViewProperties>(
  viewType: ViewType = ViewType.Line
): NewView<T> {
  const creator = NEW_VIEW_CREATORS[viewType]

  if (!creator) {
    throw new Error(`no view creator implemented for view of type ${viewType}`)
  }

  return creator()
}

// To convert from a view with `ViewType` `T` to a view with `ViewType` `R`,
// lookup the conversion function stored at the `VIEW_CONVERSIONS[T][R]` index.
// Most conversions are not yet defined---we should define them as we need.
// This matrix should only be consumed via the `convertView` helper.
const VIEW_CONVERSIONS = {
  [ViewType.Bar]: {
    [ViewType.Bar]: (view: View<BarChartView>): View<BarChartView> => view,
    [ViewType.Line]: (view: View<BarChartView>): View<LineView> => ({
      ...view,
      properties: {
        ...view.properties,
        type: ViewType.Line,
      },
    }),
    [ViewType.LinePlusSingleStat]: (
      view: View<BarChartView>
    ): View<LinePlusSingleStatView> => ({
      ...view,
      properties: {
        ...view.properties,
        type: ViewType.LinePlusSingleStat,
        prefix: '',
        suffix: '',
        decimalPlaces: {
          isEnforced: true,
          digits: 2,
        },
      },
    }),
  },
}

export function convertView<T extends View | NewView>(
  oldView: T,
  outType: ViewType
): T {
  const inType = oldView.properties.type

  let newView: any

  if (VIEW_CONVERSIONS[inType] && VIEW_CONVERSIONS[inType][outType]) {
    newView = VIEW_CONVERSIONS[inType][outType](oldView)
  } else if (NEW_VIEW_CREATORS[outType]) {
    newView = NEW_VIEW_CREATORS[outType]()
  } else {
    throw new Error(
      `cannot convert view of type "${inType}" to view of type "${outType}"`
    )
  }

  const oldViewQueries = get(oldView, 'properties.queries')
  const newViewQueries = get(newView, 'properties.queries')

  if (oldViewQueries && newViewQueries) {
    newView.properties.queries = cloneDeep(oldViewQueries)
  }

  newView.name = oldView.name
  newView.id = (oldView as any).id
  newView.links = (oldView as any).links

  return newView
}

export function replaceQuery<T extends View | NewView>(
  view: T,
  text,
  type = InfluxLanguage.Flux
): T {
  const anyView: any = view

  if (!anyView.properties.queries) {
    return
  }

  return {
    ...anyView,
    properties: {
      ...anyView.properties,
      queries: [{type, text, source: ''}],
    },
  }
}
