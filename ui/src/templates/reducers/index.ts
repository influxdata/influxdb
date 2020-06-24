import {produce} from 'immer'
import {
  Action,
  ADD_TEMPLATE_SUMMARY,
  POPULATE_TEMPLATE_SUMMARIES,
  REMOVE_TEMPLATE_SUMMARY,
  SET_ACTIVE_COMMUNITY_TEMPLATE,
  SET_EXPORT_TEMPLATE,
  SET_TEMPLATE_SUMMARY,
  SET_TEMPLATES_STATUS,
} from 'src/templates/actions/creators'
import {
  CommunityTemplate,
  ResourceType,
  RemoteDataState,
  TemplateSummary,
  TemplatesState,
} from 'src/types'
import {
  addResource,
  removeResource,
  setResource,
  setResourceAtID,
} from 'src/resources/reducers/helpers'

export const defaultState = (): TemplatesState => ({
  activeCommunityTemplate: {} as CommunityTemplate,
  status: RemoteDataState.NotStarted,
  byID: {},
  allIDs: [],
  exportTemplate: {
    status: RemoteDataState.NotStarted,
    item: null,
  },
})

export const templatesReducer = (
  state: TemplatesState = defaultState(),
  action: Action
): TemplatesState =>
  produce(state, draftState => {
    switch (action.type) {
      case POPULATE_TEMPLATE_SUMMARIES: {
        setResource<TemplateSummary>(draftState, action, ResourceType.Templates)

        return
      }

      case SET_TEMPLATES_STATUS: {
        const {status} = action
        draftState.status = status
        return
      }

      case SET_TEMPLATE_SUMMARY: {
        setResourceAtID<TemplateSummary>(
          draftState,
          action,
          ResourceType.Templates
        )

        return
      }

      case SET_ACTIVE_COMMUNITY_TEMPLATE: {
        const {template} = action

        const activeCommunityTemplate: CommunityTemplate = {}
        activeCommunityTemplate.dashboards = template.dashboards || []
        activeCommunityTemplate.telegrafConfigs = template.telegrafConfigs || []
        activeCommunityTemplate.buckets = template.buckets || []
        activeCommunityTemplate.checks = template.checks || []
        activeCommunityTemplate.variables = template.variables || []
        activeCommunityTemplate.notificationRules =
          template.notificationRules || []
        activeCommunityTemplate.labels = template.labels || []

        draftState.activeCommunityTemplate = activeCommunityTemplate
        return
      }

      case SET_EXPORT_TEMPLATE: {
        const {status, item} = action
        draftState.exportTemplate.status = status

        if (item) {
          draftState.exportTemplate.item = item
        } else {
          draftState.exportTemplate.item = null
        }
        return
      }

      case REMOVE_TEMPLATE_SUMMARY: {
        removeResource<TemplateSummary>(draftState, action)

        return
      }

      case ADD_TEMPLATE_SUMMARY: {
        addResource<TemplateSummary>(draftState, action, ResourceType.Templates)

        return
      }
    }
  })

export default templatesReducer
