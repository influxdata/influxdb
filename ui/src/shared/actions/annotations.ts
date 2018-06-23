import * as api from 'src/shared/apis/annotation'
import {Dispatch} from 'redux'
import * as Types from 'src/types/modules'

export const editingAnnotation = (): Types.Annotations.Actions.EditingAnnotationAction => ({
  type: 'EDITING_ANNOTATION',
})

export const dismissEditingAnnotation = (): Types.Annotations.Actions.DismissEditingAnnotationAction => ({
  type: 'DISMISS_EDITING_ANNOTATION',
})

export const addingAnnotation = (): Types.Annotations.Actions.AddingAnnotationAction => ({
  type: 'ADDING_ANNOTATION',
})

export const addingAnnotationSuccess = (): Types.Annotations.Actions.AddingAnnotationSuccessAction => ({
  type: 'ADDING_ANNOTATION_SUCCESS',
})

export const dismissAddingAnnotation = (): Types.Annotations.Actions.DismissAddingAnnotationAction => ({
  type: 'DISMISS_ADDING_ANNOTATION',
})

export const mouseEnterTempAnnotation = (): Types.Annotations.Actions.MouseEnterTempAnnotationAction => ({
  type: 'MOUSEENTER_TEMP_ANNOTATION',
})

export const mouseLeaveTempAnnotation = (): Types.Annotations.Actions.MouseLeaveTempAnnotationAction => ({
  type: 'MOUSELEAVE_TEMP_ANNOTATION',
})

export const loadAnnotations = (
  annotations: Types.Annotations.Data.AnnotationInterface[]
): Types.Annotations.Actions.LoadAnnotationsAction => ({
  type: 'LOAD_ANNOTATIONS',
  payload: {
    annotations,
  },
})

export const updateAnnotation = (
  annotation: Types.Annotations.Data.AnnotationInterface
): Types.Annotations.Actions.UpdateAnnotationAction => ({
  type: 'UPDATE_ANNOTATION',
  payload: {
    annotation,
  },
})

export const deleteAnnotation = (
  annotation: Types.Annotations.Data.AnnotationInterface
): Types.Annotations.Actions.DeleteAnnotationAction => ({
  type: 'DELETE_ANNOTATION',
  payload: {
    annotation,
  },
})

export const addAnnotation = (
  annotation: Types.Annotations.Data.AnnotationInterface
): Types.Annotations.Actions.AddAnnotationAction => ({
  type: 'ADD_ANNOTATION',
  payload: {
    annotation,
  },
})

export const addAnnotationAsync = (
  createUrl: string,
  annotation: Types.Annotations.Data.AnnotationInterface
) => async dispatch => {
  dispatch(addAnnotation(annotation))
  const savedAnnotation = await api.createAnnotation(createUrl, annotation)
  dispatch(addAnnotation(savedAnnotation))
  dispatch(deleteAnnotation(annotation))
}

export const getAnnotationsAsync: Types.Annotations.Actions.GetAnnotationsDispatcher = (
  indexUrl: string,
  {since, until}: Types.Annotations.Data.AnnotationRange
): Types.Annotations.Actions.GetAnnotationsThunk => async (
  dispatch: Dispatch<Types.Annotations.Actions.LoadAnnotationsAction>
): Promise<void> => {
  const annotations = await api.getAnnotations(indexUrl, since, until)
  dispatch(loadAnnotations(annotations))
}

export const deleteAnnotationAsync = (
  annotation: Types.Annotations.Data.AnnotationInterface
) => async dispatch => {
  await api.deleteAnnotation(annotation)
  dispatch(deleteAnnotation(annotation))
}

export const updateAnnotationAsync = (
  annotation: Types.Annotations.Data.AnnotationInterface
) => async dispatch => {
  await api.updateAnnotation(annotation)
  dispatch(updateAnnotation(annotation))
}
