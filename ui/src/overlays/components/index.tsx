import OverlayHandler, {
  RouteOverlay,
} from 'src/overlays/components/RouteOverlay'

export const AddNoteOverlay = RouteOverlay(
  OverlayHandler as any,
  'add-note',
  (history, params) => {
    history.push(`/orgs/${params.orgID}/dashboards/${params.dashboardID}`)
  }
)
export const EditNoteOverlay = RouteOverlay(
  OverlayHandler as any,
  'edit-note',
  (history, params) => {
    history.push(`/orgs/${params.orgID}/dashboards/${params.dashboardID}`)
  }
)
export const AllAccessTokenOverlay = RouteOverlay(
  OverlayHandler as any,
  'add-master-token',
  (history, params) => {
    history.push(`/orgs/${params.orgID}/load-data/tokens`)
  }
)
export const BucketsTokenOverlay = RouteOverlay(
  OverlayHandler as any,
  'add-token',
  (history, params) => {
    history.push(`/orgs/${params.orgID}/load-data/tokens`)
  }
)
export const TelegrafConfigOverlay = RouteOverlay(
  OverlayHandler as any,
  'telegraf-config',
  (history, params) => {
    history.push(`/orgs/${params.orgID}/load-data/telegrafs`)
  }
)

export const TelegrafOutputOverlay = RouteOverlay(
  OverlayHandler as any,
  'telegraf-output',
  (history, params) => {
    history.push(`/orgs/${params.orgID}/load-data/telegrafs`)
  }
)
