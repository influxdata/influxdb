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
} from 'src/types/v2/dashboards'

function defaultView() {
  return {
    id: '',
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
        label: '',
        prefix: '',
        suffix: '',
        base: '10',
        scale: 'linear',
      },
      y: {
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
  [ViewType.Bar]: (): View<BarChartView> => ({
    ...defaultView(),
    properties: {
      ...defaultLineViewProperties(),
      type: ViewType.Bar,
      shape: ViewShape.ChronografV2,
    },
  }),
  [ViewType.Line]: (): View<LineView> => ({
    ...defaultView(),
    properties: {
      ...defaultLineViewProperties(),
      type: ViewType.Line,
      shape: ViewShape.ChronografV2,
    },
  }),
  [ViewType.Stacked]: (): View<StackedView> => ({
    ...defaultView(),
    properties: {
      ...defaultLineViewProperties(),
      type: ViewType.Stacked,
      shape: ViewShape.ChronografV2,
    },
  }),
  [ViewType.StepPlot]: (): View<StepPlotView> => ({
    ...defaultView(),
    properties: {
      ...defaultLineViewProperties(),
      type: ViewType.StepPlot,
      shape: ViewShape.ChronografV2,
    },
  }),
  [ViewType.SingleStat]: (): View<SingleStatView> => ({
    ...defaultView(),
    properties: {
      ...defaultGaugeViewProperties(),
      type: ViewType.SingleStat,
      shape: ViewShape.ChronografV2,
    },
  }),
  [ViewType.Gauge]: (): View<GaugeView> => ({
    ...defaultView(),
    properties: {
      ...defaultGaugeViewProperties(),
      type: ViewType.Gauge,
      shape: ViewShape.ChronografV2,
    },
  }),
  [ViewType.LinePlusSingleStat]: (): View<LinePlusSingleStatView> => ({
    ...defaultView(),
    properties: {
      ...defaultLineViewProperties(),
      ...defaultGaugeViewProperties(),
      type: ViewType.LinePlusSingleStat,
      shape: ViewShape.ChronografV2,
    },
  }),
  [ViewType.Table]: (): View<TableView> => ({
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
  [ViewType.Markdown]: (): View<MarkdownView> => ({
    ...defaultView(),
    properties: {
      type: ViewType.Markdown,
      shape: ViewShape.ChronografV2,
      text: '',
    },
  }),
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

export function convertView(view: View, outType: ViewType): View {
  const inType = view.properties.type

  if (VIEW_CONVERSIONS[inType] && VIEW_CONVERSIONS[inType][outType]) {
    return VIEW_CONVERSIONS[inType][outType](view)
  }

  if (NEW_VIEW_CREATORS[outType]) {
    return NEW_VIEW_CREATORS[outType]()
  }

  throw new Error(
    `cannot convert view of type "${inType}" to view of type "${outType}"`
  )
}
