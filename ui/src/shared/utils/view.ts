// Constants
import {DEFAULT_LINE_COLORS} from 'src/shared/constants/graphColorPalettes'
import {DEFAULT_CELL_NAME} from 'src/dashboards/constants/index'
import {
  DEFAULT_GAUGE_COLORS,
  DEFAULT_THRESHOLDS_LIST_COLORS,
} from 'src/shared/constants/thresholds'

// Types
import {ViewType, ViewShape} from 'src/types/v2'
import {HistogramPosition} from 'src/minard'
import {
  XYView,
  XYViewGeom,
  HistogramView,
  LinePlusSingleStatView,
  SingleStatView,
  TableView,
  GaugeView,
  MarkdownView,
  NewView,
  ViewProperties,
  DashboardQuery,
  InfluxLanguage,
  QueryEditMode,
} from 'src/types/v2/dashboards'

function defaultView() {
  return {
    name: DEFAULT_CELL_NAME,
  }
}

export function defaultViewQuery(): DashboardQuery {
  return {
    text: '',
    type: InfluxLanguage.Flux,
    sourceID: '',
    editMode: QueryEditMode.Builder,
    builderConfig: {
      buckets: [],
      tags: [{key: '_measurement', values: []}],
      functions: [],
    },
  }
}

function defaultLineViewProperties() {
  return {
    queries: [defaultViewQuery()],
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
    queries: [defaultViewQuery()],
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

function defaultSingleStatViewProperties() {
  return {
    queries: [defaultViewQuery()],
    colors: DEFAULT_THRESHOLDS_LIST_COLORS,
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
  [ViewType.Histogram]: (): NewView<HistogramView> => ({
    ...defaultView(),
    properties: {
      queries: [],
      type: ViewType.Histogram,
      shape: ViewShape.ChronografV2,
      xColumn: '_value',
      fillColumns: null,
      position: HistogramPosition.Stacked,
      binCount: 30,
      colors: DEFAULT_LINE_COLORS,
      note: '',
      showNoteWhenEmpty: false,
    },
  }),
  [ViewType.SingleStat]: (): NewView<SingleStatView> => ({
    ...defaultView(),
    properties: {
      ...defaultSingleStatViewProperties(),
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
      ...defaultSingleStatViewProperties(),
      type: ViewType.LinePlusSingleStat,
      shape: ViewShape.ChronografV2,
    },
  }),
  [ViewType.Table]: (): NewView<TableView> => ({
    ...defaultView(),
    properties: {
      type: ViewType.Table,
      shape: ViewShape.ChronografV2,
      queries: [defaultViewQuery()],
      colors: DEFAULT_THRESHOLDS_LIST_COLORS,
      tableOptions: {
        verticalTimeAxis: true,
        sortBy: null,
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
