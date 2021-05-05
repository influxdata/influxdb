// Constants
import {INFERNO, NINETEEN_EIGHTY_FOUR} from '@influxdata/giraffe'
import {DEFAULT_LINE_COLORS} from 'src/shared/constants/graphColorPalettes'
import {DEFAULT_CELL_NAME} from 'src/dashboards/constants'
import {
  DEFAULT_GAUGE_COLORS,
  DEFAULT_THRESHOLDS_LIST_COLORS,
  DEFAULT_THRESHOLDS_TABLE_COLORS,
} from 'src/shared/constants/thresholds'
import {DEFAULT_CHECK_EVERY} from 'src/alerting/constants'
import {
  DEFAULT_FILLVALUES,
  AGG_WINDOW_AUTO,
} from 'src/timeMachine/constants/queryBuilder'

// Types
import {
  Axis,
  Base,
  BuilderConfig,
  CheckType,
  CheckViewProperties,
  Color,
  DashboardQuery,
  GaugeViewProperties,
  HeatmapViewProperties,
  HistogramViewProperties,
  LinePlusSingleStatProperties,
  MosaicViewProperties,
  MarkdownViewProperties,
  NewView,
  RemoteDataState,
  ScatterViewProperties,
  SingleStatViewProperties,
  TableViewProperties,
  ViewProperties,
  ViewType,
  XYViewProperties,
  BandViewProperties,
} from 'src/types'

export const defaultView = (name: string = DEFAULT_CELL_NAME) => {
  return {
    name,
    status: RemoteDataState.Done,
  }
}

export function defaultViewQuery(): DashboardQuery {
  return {
    name: '',
    text: '',
    editMode: 'builder',
    builderConfig: defaultBuilderConfig(),
  }
}

export function defaultBuilderConfig(): BuilderConfig {
  return {
    buckets: [],
    tags: [{key: '_measurement', values: [], aggregateFunctionType: 'filter'}],
    functions: [{name: 'mean'}],
    aggregateWindow: {period: AGG_WINDOW_AUTO, fillValues: DEFAULT_FILLVALUES},
  }
}

export function defaultLineViewProperties() {
  return {
    queries: [defaultViewQuery()],
    colors: DEFAULT_LINE_COLORS as Color[],
    note: '',
    showNoteWhenEmpty: false,
    axes: {
      x: {
        bounds: ['', ''],
        label: '',
        prefix: '',
        suffix: '',
        base: '10',
        scale: 'linear',
      } as Axis,
      y: {
        bounds: ['', ''],
        label: '',
        prefix: '',
        suffix: '',
        base: '10' as Base,
        scale: 'linear',
      } as Axis,
    },
  }
}

export function defaultBandViewProperties() {
  return {
    queries: [defaultViewQuery()],
    colors: DEFAULT_LINE_COLORS as Color[],
    note: '',
    showNoteWhenEmpty: false,
    axes: {
      x: {
        bounds: ['', ''],
        label: '',
        prefix: '',
        suffix: '',
        scale: 'linear',
      } as Axis,
      y: {
        bounds: ['', ''],
        label: '',
        prefix: '',
        suffix: '',
        scale: 'linear',
      } as Axis,
    },
  }
}

function defaultGaugeViewProperties() {
  return {
    queries: [defaultViewQuery()],
    colors: DEFAULT_GAUGE_COLORS as Color[],
    prefix: '',
    tickPrefix: '',
    suffix: '',
    tickSuffix: '',
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
    colors: DEFAULT_THRESHOLDS_LIST_COLORS as Color[],
    prefix: '',
    tickPrefix: '',
    suffix: '',
    tickSuffix: '',
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
  xy: (): NewView<XYViewProperties> => ({
    ...defaultView(),
    properties: {
      ...defaultLineViewProperties(),
      type: 'xy',
      shape: 'chronograf-v2',
      geom: 'line',
      xColumn: null,
      yColumn: null,
      position: 'overlaid',
    },
  }),
  band: (): NewView<BandViewProperties> => ({
    ...defaultView(),
    properties: {
      ...defaultBandViewProperties(),
      type: 'band',
      shape: 'chronograf-v2',
      geom: 'line',
      xColumn: null,
      yColumn: null,
    },
  }),
  histogram: (): NewView<HistogramViewProperties> => ({
    ...defaultView(),
    properties: {
      queries: [],
      type: 'histogram',
      shape: 'chronograf-v2',
      xColumn: '_value',
      xDomain: null,
      xAxisLabel: '',
      fillColumns: null,
      position: 'stacked',
      binCount: 30,
      colors: DEFAULT_LINE_COLORS as Color[],
      note: '',
      showNoteWhenEmpty: false,
    },
  }),
  heatmap: (): NewView<HeatmapViewProperties> => ({
    ...defaultView(),
    properties: {
      queries: [],
      type: 'heatmap',
      shape: 'chronograf-v2',
      xColumn: null,
      yColumn: null,
      xDomain: null,
      yDomain: null,
      xAxisLabel: '',
      yAxisLabel: '',
      xPrefix: '',
      xSuffix: '',
      yPrefix: '',
      ySuffix: '',
      colors: INFERNO,
      binSize: 10,
      note: '',
      showNoteWhenEmpty: false,
    },
  }),
  'single-stat': (): NewView<SingleStatViewProperties> => ({
    ...defaultView(),
    properties: {
      ...defaultSingleStatViewProperties(),
      type: 'single-stat',
      shape: 'chronograf-v2',
    },
  }),
  gauge: (): NewView<GaugeViewProperties> => ({
    ...defaultView(),
    properties: {
      ...defaultGaugeViewProperties(),
      type: 'gauge',
      shape: 'chronograf-v2',
    },
  }),
  'line-plus-single-stat': (): NewView<LinePlusSingleStatProperties> => ({
    ...defaultView(),
    properties: {
      ...defaultLineViewProperties(),
      ...defaultSingleStatViewProperties(),
      type: 'line-plus-single-stat',
      shape: 'chronograf-v2',
      xColumn: null,
      yColumn: null,
      position: 'overlaid',
    },
  }),
  table: (): NewView<TableViewProperties> => ({
    ...defaultView(),
    properties: {
      type: 'table',
      shape: 'chronograf-v2',
      queries: [defaultViewQuery()],
      colors: DEFAULT_THRESHOLDS_TABLE_COLORS as Color[],
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
  markdown: (): NewView<MarkdownViewProperties> => ({
    ...defaultView(),
    properties: {
      type: 'markdown',
      shape: 'chronograf-v2',
      note: '',
    },
  }),
  scatter: (): NewView<ScatterViewProperties> => ({
    ...defaultView(),
    properties: {
      type: 'scatter',
      shape: 'chronograf-v2',
      queries: [defaultViewQuery()],
      colors: NINETEEN_EIGHTY_FOUR,
      note: '',
      showNoteWhenEmpty: false,
      fillColumns: null,
      symbolColumns: null,
      xColumn: null,
      xDomain: null,
      yColumn: null,
      yDomain: null,
      xAxisLabel: '',
      yAxisLabel: '',
      xPrefix: '',
      xSuffix: '',
      yPrefix: '',
      ySuffix: '',
    },
  }),
  mosaic: (): NewView<MosaicViewProperties> => ({
    ...defaultView(),
    properties: {
      type: 'mosaic',
      shape: 'chronograf-v2',
      queries: [defaultViewQuery()],
      colors: NINETEEN_EIGHTY_FOUR,
      note: '',
      showNoteWhenEmpty: false,
      fillColumns: null,
      xColumn: null,
      xDomain: null,
      ySeriesColumns: null,
      yDomain: null,
      xAxisLabel: '',
      yAxisLabel: '',
      xPrefix: '',
      xSuffix: '',
      yPrefix: '',
      ySuffix: '',
    },
  }),
  threshold: (): NewView<CheckViewProperties> => ({
    ...defaultView('check'),
    properties: {
      type: 'check',
      shape: 'chronograf-v2',
      checkID: '',
      queries: [
        {
          name: '',
          text: '',
          editMode: 'builder',
          builderConfig: {
            buckets: [],
            tags: [
              {
                key: '_measurement',
                values: [],
                aggregateFunctionType: 'filter',
              },
            ],
            functions: [{name: 'mean'}],
            aggregateWindow: {
              period: DEFAULT_CHECK_EVERY,
              fillValues: DEFAULT_FILLVALUES,
            },
          },
        },
      ],
      colors: DEFAULT_LINE_COLORS as Color[],
    },
  }),
  deadman: (): NewView<CheckViewProperties> => ({
    ...defaultView('check'),
    properties: {
      type: 'check',
      shape: 'chronograf-v2',
      checkID: '',
      queries: [
        {
          name: '',
          text: '',
          editMode: 'builder',
          builderConfig: {
            buckets: [],
            tags: [
              {
                key: '_measurement',
                values: [],
                aggregateFunctionType: 'filter',
              },
            ],
            functions: [],
          },
        },
      ],
      colors: DEFAULT_LINE_COLORS as Color[],
    },
  }),
  custom: (): NewView<TableViewProperties> => ({
    ...defaultView(),
    properties: {
      type: 'table',
      shape: 'chronograf-v2',
      queries: [],
      colors: DEFAULT_THRESHOLDS_LIST_COLORS as Color[],
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
}

type CreateViewType = ViewType | CheckType

export function createView<T extends ViewProperties = ViewProperties>(
  viewType: CreateViewType = 'xy'
): NewView<T> {
  const creator = NEW_VIEW_CREATORS[viewType]

  if (!creator) {
    throw new Error(`no view creator implemented for view of type ${viewType}`)
  }

  return creator() as NewView<T>
}
