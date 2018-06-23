import * as api from 'src/shared/apis/annotation'
import {Dispatch} from 'redux'
import * as AnnotationData from 'src/types/annotations'
import * as AnnotationActions from 'src/types/actions/annotations'

export const editingAnnotation = (): AnnotationActions.EditingAnnotationAction => ({
  type: 'EDITING_ANNOTATION',
})

export const dismissEditingAnnotation = (): AnnotationActions.DismissEditingAnnotationAction => ({
  type: 'DISMISS_EDITING_ANNOTATION',
})

export const addingAnnotation = (): AnnotationActions.AddingAnnotationAction => ({
  type: 'ADDING_ANNOTATION',
})

export const addingAnnotationSuccess = (): AnnotationActions.AddingAnnotationSuccessAction => ({
  type: 'ADDING_ANNOTATION_SUCCESS',
})

export const dismissAddingAnnotation = (): AnnotationActions.DismissAddingAnnotationAction => ({
  type: 'DISMISS_ADDING_ANNOTATION',
})

export const mouseEnterTempAnnotation = (): AnnotationActions.MouseEnterTempAnnotationAction => ({
  type: 'MOUSEENTER_TEMP_ANNOTATION',
})

export const mouseLeaveTempAnnotation = (): AnnotationActions.MouseLeaveTempAnnotationAction => ({
  type: 'MOUSELEAVE_TEMP_ANNOTATION',
})

export const loadAnnotations = (
  annotations: AnnotationData.AnnotationInterface[]
): AnnotationActions.LoadAnnotationsAction => ({
  type: 'LOAD_ANNOTATIONS',
  payload: {
    annotations,
  },
})

export const updateAnnotation = (
  annotation: AnnotationData.AnnotationInterface
): AnnotationActions.UpdateAnnotationAction => ({
  type: 'UPDATE_ANNOTATION',
  payload: {
    annotation,
  },
})

export const deleteAnnotation = (
  annotation: AnnotationData.AnnotationInterface
): AnnotationActions.DeleteAnnotationAction => ({
  type: 'DELETE_ANNOTATION',
  payload: {
    annotation,
  },
})

export const addAnnotation = (
  annotation: AnnotationData.AnnotationInterface
): AnnotationActions.AddAnnotationAction => ({
  type: 'ADD_ANNOTATION',
  payload: {
    annotation,
  },
})

export const addAnnotationAsync = (
  createUrl: string,
  annotation: AnnotationData.AnnotationInterface
) => async dispatch => {
  dispatch(addAnnotation(annotation))
  const savedAnnotation = await api.createAnnotation(createUrl, annotation)
  dispatch(addAnnotation(savedAnnotation))
  dispatch(deleteAnnotation(annotation))
}

export const getAnnotationsAsync: AnnotationActions.GetAnnotationsDispatcher = (
  indexUrl: string,
  {since, until}: AnnotationData.AnnotationRange
): AnnotationActions.GetAnnotationsThunk => async (
  dispatch: Dispatch<AnnotationActions.LoadAnnotationsAction>
): Promise<void> => {
  const annotations = await api.getAnnotations(indexUrl, since, until)
  dispatch(loadAnnotations(annotations))
}

export const deleteAnnotationAsync = (
  annotation: AnnotationData.AnnotationInterface
) => async dispatch => {
  await api.deleteAnnotation(annotation)
  dispatch(deleteAnnotation(annotation))
}

export const updateAnnotationAsync = (
  annotation: AnnotationData.AnnotationInterface
) => async dispatch => {
  await api.updateAnnotation(annotation)
  dispatch(updateAnnotation(annotation))
}
